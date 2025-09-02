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

use Closure;
use Exception;
use InvalidArgumentException;
use RuntimeException;
use UnexpectedValueException;

declare(ticks=1);

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
 * Example:
 * <code>
 *   $pool = new ProcessPool(16);
 *   for ($i=0; $i<100; $i++) {
 *       $pool->apply(function($parent) use ($i) {
 *           echo "$i running...\n";
 *           mt_srand(); // must re-seed for each child
 *           $rand = mt_rand(1000000, 2000000);
 *           usleep($rand);
 *           return $i . ' : slept for ' . ($rand / 1000000) . ' seconds';
 *       });
 *   }
 *   while ($pool->getPending()) {
 *       try {
 *           $result = $pool->get(1);    // timeout in 1 second
 *           echo "GOT: ", $result, "\n";
 *       } catch (ProcessPoolException $e) {
 *           // timeout
 *       }
 *   }
 *  </code>
 */
class ProcessPool
{
    /** @var Integer Maximum workers allowed at once */
    protected int $max;

    /** @var boolean If true workers will fork. Otherwise they will run synchronously */
    protected bool $fork;

    /** @var Integer Total results collected */
    protected int $count = 0;

    /** @var array Pending processes that have not been started yet */
    protected array $pending = [];

    /** @var array Processes that have been started */
    protected array $workers = [];

    /** @var array Results that have been collected */
    protected array $results = [];

    /** @var Closure|null Function to call every time a child is forked */
    protected ?Closure $createCallback = null;

    protected ?int $sendSize = null;

    protected ?int $recvSize = null;

    /** @var array children PID's that died prematurely */
    private array $caught = [];

    /** @var boolean Is the signal handler initialized? */
    private bool $initialized = false;

//    private static array $instance = [];

    public function __construct(int $max = 1, bool $fork = true)
    {
        //$pid = getmypid();
        //if (isset(self::$instance[$pid])) {
        //    $caller = debug_backtrace();
        //    throw new ProcessPoolException("Cannot instantiate more than 1 ProcessPool in the same process in {$caller[0]['file']} line {$caller[0]['line']}");
        //}
        //self::$instance[$pid] = $this;
        $this->max = $max;
        $this->fork = $fork;
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
     * @param bool $force Force initialization even if already initialized
     */
    private function init(bool $force = false): void
    {
        if (!function_exists('pcntl_signal') || ($this->initialized && !$force)) {
            return;
        }
        $this->initialized = true;
        pcntl_signal(SIGCHLD, [$this, 'signalHandler']);
    }

    private function uninit(): void
    {
        if (!function_exists('pcntl_signal') || !$this->initialized) {
            return;
        }
        $this->initialized = false;
        pcntl_signal(SIGCHLD, SIG_DFL);
    }

    public function signalHandler(int $signo): void
    {
        if ($signo == SIGCHLD) {
            $this->reaper();
        }
    }

    /**
     * Reap any dead children
     */
    public function reaper($pid = null, $status = null): void
    {
        if ($pid === null) {
            $pid = pcntl_waitpid(-1, $status, WNOHANG);
        }

        while ($pid > 0) {
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

    public function setSendSize(?int $size): static
    {
        $this->sendSize = $size;
        return $this;
    }

    public function getSendSize(): ?int
    {
        return $this->sendSize;
    }

    public function setRecvSize(?int $size): static
    {
        $this->recvSize = $size;
        return $this;
    }

    public function getRecvSize(): ?int
    {
        return $this->recvSize;
    }

    /**
     * Wait for any child to be ready
     *
     * @param integer|null $timeout Timeout to wait (fractional seconds)
     *
     * @return array|null Returns array of sockets ready to be READ or null
     */
    public function wait(?int $timeout = null): ?array
    {
        $x = null;                      // trash var needed for socket_select
        $startTime = microtime(true);
        while (true) {
            $this->apply();                         // maintain worker queue

            // check each child socket pair for a new result
            $read = array_map(fn(array $w) => $w['socket'], $this->workers);
            // it's possible for no workers/sockets to be present due to REAPING
            if (!empty($read)) {
                $ok = @socket_select($read, $x, $x, $timeout);
                if ($ok !== false and $ok > 0) {
                    return $read;
                }
            }

            // timed out?
            if ($timeout and microtime(true) - $startTime > $timeout) {
                return null;
            }

            // no sense in waiting if we have no workers and no more pending
            if (empty($this->workers) and empty($this->pending)) {
                return null;
            }
        }
    }

    /**
     * Return the next available result.
     *
     * Blocks unless a $timeout is specified.
     *
     * @param integer|null $timeout Timeout in fractional seconds if no results are available.
     *
     * @return mixed Returns next child response or null on timeout
     * @throws ProcessPoolException On timeout if $nullOnTimeout is false
     */
    public function get(?int $timeout = null, bool $nullOnTimeout = false): mixed
    {
        $startTime = microtime(true);
        while ($this->getPending()) {
            // return the next result
            if ($this->hasResult()) {
                return $this->getResult();
            }

            // wait for the next result
            $ready = $this->wait($timeout);
            if (is_array($ready)) {
                foreach ($ready as $socket) {
                    $this->applySocketRecvOptions($socket);
                    $res = self::socket_fetch($socket);
                    if ($res !== null) {
                        $this->results[] = $res;
                        $this->count++;
                    }
                }
                if ($this->hasResult()) {
                    return $this->getResult();
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

        return null;
    }

    /**
     * Return results from all workers.
     *
     * Does not return until all pending workers are complete or the $timeout
     * is reached.
     *
     * @param integer|null $timeout Timeout in fractional seconds if no results are available.
     *
     * @return array|null Returns an array of results
     * @throws ProcessPoolException On timeout if $nullOnTimeout is false
     */
    public function getAll(?int $timeout = null, bool $nullOnTimeout = false): ?array
    {
        $results = [];
        $startTime = microtime(true);
        while ($this->getPending()) {
            try {
                $res = $this->get($timeout);
                if ($res !== null) {
                    $results[] = $res;
                }
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

    public function hasResult(): bool
    {
        return !empty($this->results);
    }

    /**
     * Return the next available result or null if none are available.
     *
     * This does not wait or manage the worker queue.
     */
    public function getResult(): mixed
    {
        if (empty($this->results)) {
            return null;
        }
        return array_shift($this->results);
    }

    /**
     * Apply a worker to the working or pending queue
     *
     * @param Callable|null $func Callback function to fork into.
     *
     * @return ProcessPool
     */
    public function apply(?callable $func = null): static
    {
        // add new function to pending queue
        if ($func !== null) {
            if ($func instanceof Closure or $func instanceof ProcessInterface or is_callable($func)) {
                $this->pending[] = func_get_args();
            } else {
                throw new UnexpectedValueException("Parameter 1 in ProcessPool#apply must be a Closure or callable");
            }
        }

        // start a new worker if our current worker queue is low
        if (!empty($this->pending) and count($this->workers) < $this->max) {
            call_user_func_array([$this, 'create'], array_shift($this->pending));
        }

        return $this;
    }

    /**
     * Create a new worker.
     *
     * If forking is disabled this will BLOCK.
     *
     * @param callable $func Callback function.
     * @param mixed Any extra parameters are passed to the callback function.
     *
     * @throws RuntimeException if the child can not be forked.
     */
    protected function create(callable $func/*, ...*/): void
    {
        // create a socket pair before forking so our child process can write to the PARENT.
        $sockets = [];
        $domain = strtoupper(substr(PHP_OS, 0, 3)) == 'WIN' ? AF_INET : AF_UNIX;
        if (socket_create_pair($domain, SOCK_STREAM, 0, $sockets) === false) {
            throw new RuntimeException("socket_create_pair failed: " . socket_strerror(socket_last_error()));
        }
        list($child, $parent) = $sockets; // just to make the code below more readable
        unset($sockets);

        $args = array_merge([$parent], array_slice(func_get_args(), 1));

        $this->init();                  // make sure signal handler is installed

        if ($this->fork && function_exists('pcntl_fork')) {
            $pid = pcntl_fork();
            if ($pid == -1) {
                throw new RuntimeException("Could not fork");
            }

            if ($pid > 0) {
                // PARENT PROCESS; Just track the child and return
                socket_close($parent);
                $this->workers[$pid] = [
                    'pid'    => $pid,
                    'socket' => $child,
                ];
                // don't pass $parent to callback
                $this->doOnCreate(array_slice($args, 1));

                // If a SIGCHLD was already caught at this point we need to
                // manually handle it to avoid a defunct process.
                if (isset($this->caught[$pid])) {
                    $this->reaper($pid, $this->caught[$pid]);
                    unset($this->caught[$pid]);
                }
            } else {
                // CHILD PROCESS; execute the callback function and wait for response
                socket_close($child);
                try {
                    if ($func instanceof ProcessInterface) {
                        $result = call_user_func_array([$func, 'run'], $args);
                    } else {
                        $result = call_user_func_array($func, $args);
                    }
                    if ($result !== null) {
                        $this->applySocketSendOptions($parent);
                        self::socket_send($parent, $result);
                    }
                } catch (Exception $e) {
                    // this is kind of useless in a forking context but at
                    // least the developer can see the exception if it occurs.
                    throw $e;
                }
                exit(0);
            }
        } else {
            // forking is disabled so we simply run the child worker and wait
            // synchronously for response.
            try {
                if ($func instanceof ProcessInterface) {
                    $result = call_user_func_array([$func, 'run'], $args);
                } else {
                    $result = call_user_func_array($func, $args);
                }
                if ($result !== null) {
                    //$this->results[] = $result;
                    $this->applySocketSendOptions($parent);
                    self::socket_send($parent, $result);
                }

                // read anything pending from the worker if they chose to write
                // to the socket instead of just returning a value.
                $x = null;
                do {
                    $read = array($child);
                    $ok = socket_select($read, $x, $x, 0);
                    if ($ok !== false and $ok > 0) {
                        $this->applySocketRecvOptions($read[0]);
                        $res = self::socket_fetch($read[0]);
                        if ($res !== null) {
                            $this->results[] = $res;
                        }
                    }
                } while ($ok);

            } catch (Exception $e) {
                // nop; we didn't fork so let the caller handle it
                throw $e;
            }
        }
    }

    protected function applySocketSendOptions($socket): void
    {
        if ($this->sendSize) {
            socket_set_option($socket, SOL_SOCKET, SO_SNDBUF, $this->sendSize);
        }
    }

    protected function applySocketRecvOptions($socket): void
    {
        if ($this->recvSize) {
            socket_set_option($socket, SOL_SOCKET, SO_RCVBUF, $this->recvSize);
        }
    }

    /**
     * Clear all pending workers from the queue.
     */
    public function clear(): static
    {
        $this->pending = array();
        return $this;
    }

    /**
     * Send a SIGTERM (or other) signal to the PID given
     */
    public function kill(int $pid, int $signo = SIGTERM): static
    {
        posix_kill($pid, $signo);
        return $this;
    }

    /**
     * Send a SIGTERM (or other) signal to all current workers
     */
    public function killAll(int $signo = SIGTERM): static
    {
        foreach ($this->workers as $w) {
            $this->kill($w['pid'], $signo);
        }
        return $this;
    }

    /**
     * Set a callback when a new forked process is created. This will allow the
     * parent to perform some sort of cleanup after every child is created.
     *
     * This is useful to reinitialize certain resources like DB connections
     * since children will inherit the parent resources.
     *
     * @param callable|null $callback Function to callback after every forked child.
     */
    public function setOnCreate(callable $callback = null): void
    {
        $this->createCallback = $callback;
    }

    protected function doOnCreate(array $args = []): void
    {
        if ($this->createCallback) {
            call_user_func_array($this->createCallback, $args);
        }
    }

    /**
     * Return the total jobs that have NOT completed yet.
     */
    public function getPending(bool $pendingOnly = false): int
    {
        if ($pendingOnly) {
            return count($this->pending);
        }
        return count($this->pending) + count($this->workers) + count($this->results);
    }

    public function getWorkers(): int
    {
        return count($this->workers);
    }

    public function getActive(): int
    {
        return count($this->pending) + count($this->workers);
    }

    public function getCompleted(): int
    {
        return $this->count;
    }

    public function setForking(bool $fork): static
    {
        $this->fork = $fork;
        return $this;
    }

    public function setMax(int $max): static
    {
        if ($max < 1) {
            throw new InvalidArgumentException("Max value must be > 0");
        }
        $this->max = $max;
        return $this;
    }

    public function getMax(): int
    {
        return $this->max;
    }

    /**
     * Write the data to the socket in a predetermined format
     */
    public static function socket_send($socket, mixed $data): void
    {
        $serialized = serialize($data);
        $hdr = pack('N', strlen($serialized));    // 4 byte length
        $buffer = $hdr . $serialized;
        $total = strlen($buffer);
        while (true) {
            $sent = socket_write($socket, $buffer);
            if ($sent === false) {
                // @todo handle error?
                //$error = socket_strerror(socket_last_error());
                break;
            }
            if ($sent >= $total) {
                break;
            }
            $total -= $sent;
            $buffer = substr($buffer, $sent);
        }
    }

    /**
     * Read a data packet from the socket in a predetermined format.
     *
     * Blocking.
     */
    public static function socket_fetch($socket)
    {
        // read 4 byte length first
        $hdr = '';
        do {
            $read = socket_read($socket, 4 - strlen($hdr));
            if ($read === false or $read === '') {
                return null;
            }
            $hdr .= $read;
        } while (strlen($hdr) < 4);

        list($len) = array_values(unpack("N", $hdr));

        // read the full buffer
        $buffer = '';
        do {
            $read = socket_read($socket, $len - strlen($buffer));
            if ($read === false or $read == '') {
                return null;
            }
            $buffer .= $read;
        } while (strlen($buffer) < $len);

        return unserialize($buffer);
    }
}
