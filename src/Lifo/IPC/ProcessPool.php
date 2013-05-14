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

/**
 * A simple Process Pool manager for managing a list of forked processes.
 * API Inspired by the Python multiprocessing library.
 *
 * Each "process" is a function that is called via a forked child. A child
 * may return any serializable result back to the parent.
 *
 * @example
    $pool = new ProcessPool(16);
    for ($i=0; $i<100; $i++) {
        $pool->apply(function() use ($i, $output) {
            $output->writeln("$i running...");
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
        } catch (Exception $e) {
            // timeout
        }
    }
 *
 */
class ProcessPool
{
    protected $max;
    protected $pending;
    protected $workers;
    protected $results;
    protected $count;

    public function __construct($max = 1)
    {
        $this->count = 0;
        $this->max = $max;
        $this->results = array();
    }

    /**
     * Wait for any child to be ready
     *
     * @param integer $timeout Timeout to wait (microseconds)
     * @return array|null Returns array of sockets ready to be READ or null
     */
    public function wait($timeout = 0)
    {
        //if ($timeout !== null and (!is_numeric($timeout) or $timeout < 0)) {
        //    throw new \InvalidArgumentException("Invalid timeout value: Must be a positive integer");
        //}

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
            $ok = socket_select($read, $x, $x, $timeout);

            if ($ok !== false and $ok > 0) {
                return $read;
            }

            // timed out?
            if ($timeout > 0 and microtime(true) - $startTime > $timeout) {
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
     * @throws \Exception On timeout if $nullOnTimeout is false
     */
    public function get($timeout = 0, $nullOnTimeout = false)
    {
        // if we have no possible means of fetching a result then return
        if (empty($this->workers) and empty($this->results) and empty($this->pending)) {
            return false;
        }

        // return the next result; if already available
        if (!empty($this->results)) {
            return array_shift($this->results);
        }

        $status = null;
        $startTime = microtime(true);
        while (true) {
            $ready = $this->wait($timeout);

            if (is_array($ready)) {
                foreach ($ready as $socket) {
                    $this->results[] = self::socket_read($socket);
                    $this->count++;

                    // find the worker that matched this socket.
                    foreach ($this->workers as $id => $w) {
                        if ($w['socket'] === $socket) {
                            echo "Worker #$id is done.\n";
                            unset($this->workers[$id]);

                            // kill the child now. This makes the call to
                            // pcntl_waitpid much faster.
                            // @todo make this configurable; what if we don't
                            //       want children to exit immediately after a
                            //       result?
                            posix_kill($w['pid'], SIGINT);

                            // wait for child to exit (avoid "defunct" processes)
                            //pcntl_waitpid($w['pid'], $status, WUNTRACED);
                            pcntl_waitpid(-1, $status, WUNTRACED);

                            break;
                        }
                    }
                }
            }

            // return the next result
            if (!empty($this->results)) {
                return array_shift($this->results);
            }

            // timed out?
            if ($timeout > 0 and microtime(true) - $startTime > $timeout) {
                if ($nullOnTimeout) {
                    return null;
                }
                throw new \Exception("ProcessPool Timeout");
            }
        }
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
            if ($func instanceof \Closure or is_callable($func)) {
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
     * @param Closure $func Callback function.
     * @param mixed Any extra parameters are passed to the callback function.
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

        $time = microtime(true);

        // fork it good!
        $pid = pcntl_fork();
        if ($pid == -1) {
            throw new \RuntimeException("Could not fork");
        }

        if ($pid > 0) {
            // PARENT PROCESS; Just track the child and return
            socket_close($parent);
            $this->workers[$pid] = array(
                'pid' => $pid,
                'time' => $time,
                'socket' => $child,
            );
        } else {
            // CHILD PROCESS; execute the callback function and wait for response
            socket_close($child);
            try {
                $args = array_slice(func_get_args(), 1);
                $result = call_user_func_array($func, $args);
                self::socket_write($parent, $result);
            } catch (\Exception $e) {
                // nop
            }
            exit(0);    // child is done
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
    protected static function socket_write($fd, $data)
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
    protected static function socket_read($fd, $timeout = 0)
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
