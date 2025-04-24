<?php
declare(strict_types=1);

namespace LesQueue\Response;

use Override;
use Countable;
use Traversable;
use ArrayIterator;
use JsonSerializable;
use IteratorAggregate;
use LesQueue\Job\Job;

/**
 * @implements IteratorAggregate<Job>
 */
final class Jobs implements IteratorAggregate, Countable, JsonSerializable
{
    /**
     * @param array<Job> $jobs
     * @param int<0, max> $count
     */
    public function __construct(
        private readonly array $jobs,
        private readonly int $count,
    ) {}

    /**
     * @return Traversable<Job>
     */
    #[Override]
    public function getIterator(): Traversable
    {
        return new ArrayIterator($this->jobs);
    }

    #[Override]
    public function jsonSerialize(): mixed
    {
        return $this->jobs;
    }

    #[Override]
    public function count(): int
    {
        return $this->count;
    }
}
