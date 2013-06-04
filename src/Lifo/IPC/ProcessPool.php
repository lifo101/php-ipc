<?php
/**
 * This file is part of the Lifo\IPC PHP Library.
 *
 * (c) Jason Morriss <lifo2013@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */
namespace Lifo\IPC;

declare(ticks = 1);

/**
 * A simple Process Pool manager for managing a list of forked processes.
 * API Inspired by the Python multiprocessing library.
 *
 * Each "process" is a function that is called via a forked child. A child
 * may send any serializable result back to the parent or can use
 * ProcessPool::socket_send() to send multiple serializable results to the
 * $parent socket.
 *
 * Only 1 ProcessPool per process can be active at a time due to the signal
 * handler for SIGCHLD. IF you attempt to start a second pool within the same
 * process the second instance will override the SIGCHLD handler and the
 * previous ProcessPool will not reap its children properly.
 *
 * @example
    $pool = new ProcessPool(16);
    for ($i=0; $i<100; $i++) {
        $pool->apply(function($parent) use ($i) {
            echo "$i running...\n";
            mt_srand(); // must re-seed for each child
            $rand = mt_rand(1000000, 2000000);
            usleep($rand);
            return $i . ' : slept for ' . ($rand / 1000000) . ' seconds';
        });
    }
    while ($pool->getPending()) {
        try {
            $result = $pool->get(1000000); // timeout in 1 second
            echo "GOT: ", $result, "\n";
        } catch (ProcessPoolException $e) {
            // timeout
        }
    }
 *
 */
class ProcessPool
{
    /** @var Integer Maximum workers allowed at once */
    protected $max;

    /** @var boolean If true workers will fork. Otherwise they will run synchronously */
    protected $fork;

    /** @var Integer Total results collected */
    protected $count;

    /** @var array Pending processes that have not been started yet */
    protected $pending;

    /** @var array Processes that have been started */
    protected $workers;

    /** @var array Results that have been collected */
    protected $results;

    /** @var \Closure Function to call every time a child is forked */
    protected $createCallback;

    /** @var array children PID's that died prematurely */
    private   $caught;

    /** @var boolean Is the signal handler initialized? */
    private   $initialized;

    private static $instance = array();

    public function __construct($max = 1, $fork = true)
    {
        //$pid = getmypid();
        //if (isset(self::$instance[$pid])) {
        //    $caller = debug_backtrace();
        //    throw new ProcessPoolException("Cannot instantiate more than 1 ProcessPool in the same process in {$caller[0]['file']} line {$caller[0]['line']}");
        //}
        //self::$instance[$pid] = $this;
        $this->count = 0;
        $this->max = $max;
        $this->fork = $fork;
        $this->results = array();
        $this->workers = array();
        $this->pending = array();
        $this->caught = array();
        $this->initialized = false;
    }

    public function __destruct()
    {
        // make sure signal handler is removed
        $this->uninit();
        //unset(self::$instance[getmygid()]);
    }

    /**
     * Initialize the signal handler.
     *
     * Note: This will replace any current handler for SIGCHLD.
     *
     * @param boolean $force Force initialization even if already initialized
     */
    private function init($force = false)
    {
        if ($this->initialized and !$force) {
            return;
        }
        $this->initialized = true;
        pcntl_signal(SIGCHLD, array($this, 'reaper'));
    }

    private function uninit()
    {
        if (!$this->initialized) {
            return;
        }
        $this->initialized = false;
        pcntl_signal(SIGCHLD, SIG_DFL);
    }

    /**
     * Reap any dead children
     */
    public function reaper($signo, $pid = null, $status = null)
    {
        if ($pid === null) {
            $pid = pcntl_waitpid(-1, $status, WNOHANG);
        }

        while ($pid > 0) {
            //$exitCode = pcntl_wexitstatus($status);
            if (isset($this->workers[$pid])) {
                // @todo does the socket really need to be closed?
                //@socket_close($this->workers[$pid]['socket']);
                unset($this->workers[$pid]);
            } else {
                // the child died before the parent could initialize the $worker
                // queue. So we track it temporarily so we can handle it in
                // self::create().
                $this->caught[$pid] = $status;
            }
            $pid = pcntl_waitpid(-1, $status, WNOHANG);
        }
    }

    /**
     * Wait for any child to be ready
     *
     * @param integer $timeout Timeout to wait (microseconds)
     * @return array|null Returns array of sockets ready to be READ or null
     */
    public function wait($timeout = null)
    {
        // no sense in waiting if we have no workers and no more pending
        if (empty($this->workers) and empty($this->pending)) {
            return false;
        }

        $x = null;                      // trash var needed for socket_select
        $startTime = microtime(true);
        while (true) {
            $this->apply();                         // maintain worker queue

            // check each child socket pair for a new result
            $read = array_map(function($w){ return $w['socket']; }, $this->workers);
            // it's possible for no workers to be present due to REAPING
            if (empty($read)) {
                return null;
            }
            $ok = socket_select($read, $x, $x, $timeout);

            if ($ok !== false and $ok > 0) {
                return $read;
            }

            // timed out?
            if ($timeout and microtime(true) - $startTime > $timeout) {
                return null;
            }
        }
    }

    /**
     * Return the next available result.
     *
     * Blocks unless a $timeout is specified.
     *
     * @param integer $timeout Timeout in microseconds if no results are available.
     * @return mixed Returns next child response or null on timeout
     * @throws ProcessPoolException On timeout if $nullOnTimeout is false
     */
    public function get($timeout = null, $nullOnTimeout = false)
    {
        // return the next result; if already available
        if (!empty($this->results)) {
            return array_shift($this->results);
        }

        $startTime = microtime(true);
        while ($this->getPending()) {
            // return the next result
            if (!empty($this->results)) {
                return array_shift($this->results);
            }

            $ready = $this->wait($timeout);
            if (is_array($ready)) {
                foreach ($ready as $socket) {
                    $res = self::socket_fetch($socket);
                    if ($res !== null) {
                        $this->results[] = $res;
                        $this->count++;
                    }
                }
            }

            // timed out?
            if ($timeout and microtime(true) - $startTime > $timeout) {
                if ($nullOnTimeout) {
                    return null;
                }
                throw new ProcessPoolException("Timeout");
            }
        }
    }

    /**
     * Return results from all workers.
     *
     * Does not return until all pending workers are complete or the $timeout
     * is reached.
     *
     * @param integer $timeout Timeout in microseconds if no results are available.
     * @return array Returns an array of results
     * @throws ProcessPoolException On timeout if $nullOnTimeout is false
     */
    public function getAll($timeout = null, $nullOnTimeout = false)
    {
        $results = array();
        $startTime = microtime(true);
        while ($this->getPending()) {
            try {
                $results[] = $this->get($timeout);
            } catch (ProcessPoolException $e) {
                // timed out
            }

            // timed out?
            if ($timeout and microtime(true) - $startTime > $timeout) {
                if ($nullOnTimeout) {
                    return null;
                }
                throw new ProcessPoolException("Timeout");
            }
        }
        return $results;
    }

    /**
     * Apply a worker to the working or pending queue
     *
     * @param Callable $func Callback function to fork into.
     * @return ProcessPool
     */
    public function apply($func = null)
    {
        // add new function to pending queue
        if ($func !== null) {
            if ($func instanceof \Closure or $func instanceof ProcessInterface or is_callable($func)) {
                $this->pending[] = array_merge(array( $func ), array_slice(func_get_args(), 1));
            } else {
                throw new \UnexpectedValueException("Parameter 1 in ProcessPool#apply must be a Closure or callable");
            }
        }

        // start a new worker if our current worker queue is low
        if (!empty($this->pending) and count($this->workers) < $this->max) {
            call_user_func_array(array($this, 'create'), array_shift($this->pending));
        }

        return $this;
    }

    /**
     * Create a new worker.
     *
     * If forking is disabled this will BLOCK.
     *
     * @param Closure $func Callback function.
     * @param mixed Any extra parameters are passed to the callback function.
     * @throws \RuntimeException if the child can not be forked.
     */
    protected function create($func /*, ...*/)
    {
        // create a socket pair before forking so our child process can write to the PARENT.
        $sockets = array();
        $domain = strtoupper(substr(PHP_OS, 0, 3)) == 'WIN' ? AF_INET : AF_UNIX;
        if (socket_create_pair($domain, SOCK_STREAM, 0, $sockets) === false) {
            throw new \RuntimeException("socket_create_pair failed: " . socket_strerror(socket_last_error()));
        }
        list($child, $parent) = $sockets; // just to make the code below more readable
        unset($sockets);

        $args = array_merge(array($parent), array_slice(func_get_args(), 1));

        $this->init();                  // make sure signal handler is installed

        if ($this->fork) {
            $pid = pcntl_fork();
            if ($pid == -1) {
                throw new \RuntimeException("Could not fork");
            }

            if ($pid > 0) {
                // PARENT PROCESS; Just track the child and return
                socket_close($parent);
                $this->workers[$pid] = array(
                    'pid' => $pid,
                    'socket' => $child,
                );
                // don't pass $parent to callback
                $this->doOnCreate(array_slice($args, 1));

                // If a SIGCHLD was already caught at this point we need to
                // manually handle it to avoid a defunct process.
                if (isset($this->caught[$pid])) {
                    $this->reaper(SIGCHLD, $pid, $this->caught[$pid]);
                    unset($this->caught[$pid]);
                }
            } else {
                // CHILD PROCESS; execute the callback function and wait for response
                socket_close($child);
                try {
                    if ($func instanceof ProcessInterface) {
                        $result = call_user_func_array(array($func, 'run'), $args);
                    } else {
                        $result = call_user_func_array($func, $args);
                    }
                    if ($result !== null) {
                        self::socket_send($parent, $result);
                    }
                } catch (\Exception $e) {
                    // this is kind of useless in a forking context but at
                    // least the developer can see the exception if it occurs.
                    throw $e;
                }
                // @todo Is this really wise? By killing the child we prevent
                // it was closing any shared resources (like DB connections)
                // which can cause problems in the parent. But I have not
                // personally tested this to see how well it works or what its
                // side effects might be with a lot of short-lived processes.
                //posix_kill(getmypid(),9);
                exit(0);
            }
        } else {
            // forking is disabled so we simply run the child worker and wait
            // synchronously for response.
            try {
                $args = array_merge(array($parent), array_slice(func_get_args(), 1));
                if ($func instanceof ProcessInterface) {
                    $result = call_user_func_array(array($func, 'run'), $args);
                } else {
                    $result = call_user_func_array($func, $args);
                }
                if ($result !== null) {
                    $this->results[] = $result;
                }

                // read anything pending from the worker if they chose to write
                // to the socket instead of just returning a value.
                $x = null;
                do {
                    $read = array($child);
                    $ok = socket_select($read, $x, $x, 0);
                    if ($ok !== false and $ok > 0) {
                        $res = self::socket_fetch($read[0]);
                        if ($res !== null) {
                            $this->results[] = $res;
                        }
                    }
                } while ($ok);

            } catch (\Exception $e) {
                // nop; we didn't fork so let the caller handle it
                throw $e;
            }
        }
    }

    /**
     * Set a callback when a new forked process is created. This will allow the
     * parent to perform some sort of cleanup after every child is created.
     *
     * This is useful to reinitialize certain resources like DB connections
     * since children will inherit the parent resources.
     *
     * @param \Closure $callback Function to callback after every forked child.
     */
    public function setOnCreate(\Closure $callback = null)
    {
        $this->createCallback = $callback;
    }

    protected function doOnCreate($args = array())
    {
        if ($this->createCallback) {
            call_user_func_array($this->createCallback, $args);
        }
    }

    /**
     * Return the total jobs that have NOT completed yet.
     */
    public function getPending($pendingOnly = false)
    {
        if ($pendingOnly) {
            return count($this->pending);
        }
        return count($this->pending) + count($this->workers) + count($this->results);
    }

    public function getWorkers()
    {
        return count($this->workers);
    }

    public function getActive()
    {
        return count($this->pending) + count($this->workers);
    }

    public function getCompleted()
    {
        return $this->count;
    }

    public function setForking($fork)
    {
        $this->fork = $fork;
        return $this;
    }

    public function setMax($max)
    {
        if (!is_numeric($max) or $max < 1) {
            throw new \InvalidArgumentException("Max value must be > 0");
        }
        $this->max = $max;
        return $this;
    }

    public function getMax()
    {
        return $this->max;
    }

    /**
     * Write the data to the socket in a predetermined format
     */
    public static function socket_send($fd, $data)
    {
        $serialized = serialize($data);
        $len = strlen($serialized);
        $hdr = pack('N', $len);    // 4 byte length
        return socket_write($fd, $hdr . $serialized);
    }

    /**
     * Read a data packet from the socket in a predetermined format.
     *
     * Blocking.
     *
     */
    public static function socket_fetch($fd)
    {
        // read 4 byte length first
        $hdr = socket_read($fd, 4);
        if ($hdr === false or $hdr === '') {
            return null;
        }
        list($len) = array_values(unpack("N", $hdr));

        // read the full buffer
        $buffer = socket_read($fd, $len);
        if ($buffer === false or $buffer == '') {
            return null;
        }

        $data = unserialize($buffer);
        return $data;
    }
}
