PHP IPC Library
=======

**Inter Process Communication**

_This library is in its infancy. I am adding features to it as I require them in my other projects._

## Examples

### ProcessPool

The ProcessPool class provides a very simple interface to manage a list of **short-lived** children that return a single result and exit. Each child can return any value that can be [serialized][serialize].

```php
<?php
use Lifo\IPC\ProcessPool;

// create a pool with a maximum of 16 workers at a time
$pool = new ProcessPool(16);

// apply 100 processes to the pool.
for ($i=0; $i<100; $i++) {
    $pool->apply(function() use ($i) {
        mt_srand(); // must re-seed for each child
        $rand = mt_rand(1000000, 2000000);
        usleep($rand);
        return $i . ' : slept for ' . ($rand / 1000000) . ' seconds';
    });
}

// wait for all results to be finished ...
while ($pool->getPending()) {
    try {
        $result = $pool->get(1000000); // timeout in 1 second
        echo "GOT: ", $result, "\n";
    } catch (\Exception $e) {
        // timeout
    }
}
```

  [serialize]: http://php.net/serialize
