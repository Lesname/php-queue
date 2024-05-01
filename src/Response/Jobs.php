<?php
declare(strict_types=1);

namespace LessQueue\Response;

use Countable;
use Traversable;
use ArrayIterator;
use IteratorAggregate;
use LessQueue\Job\Job;

/**
 * @implements IteratorAggregate<Job>
 */
final class Jobs implements IteratorAggregate, Countable
{
    /**
     * @param array<Job> $jobs
     */
    public function __construct(
        private readonly array $jobs,
        private readonly int $count,
    ) {}

    /**
     * @return Traversable<Job>
     */
    public function getIterator(): Traversable
    {
        return new ArrayIterator($this->jobs);
    }

    public function count(): int
    {
        return $this->count;
    }
}
