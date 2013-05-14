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
 * Interface representing an activity that will be run in a separate forked
 * process.
 */
interface ProcessInterface
{
    /**
     * Method representing the process’s activity which is run in its own
     * child process.
     */
    public function run();
}
