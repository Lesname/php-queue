<?php

declare(strict_types=1);

namespace LesQueue;

use LesQueue\Job\Job;
use LesQueue\Response\Jobs;
use LesQueue\Job\Property\Identifier;
use LesQueue\Job\Property\Name;
use LesQueue\Parameter\Priority;
use LesValueObject\Composite\Paginate;
use LesValueObject\Number\Int\Date\Timestamp;
use LesValueObject\Composite\DynamicCompositeValueObject;

/**
 * @psalm-mutable
 */
interface Queue
{
    /**
     * @psalm-impure
     */
    public function publish(Name $name, DynamicCompositeValueObject $data, ?Timestamp $until = null, ?Priority $priority = null): void;

    /**
     * @psalm-impure
     */
    public function republish(Job $job, Timestamp $until, ?Priority $priority = null): void;

    /**
     * @param callable(Job $job): void $callback
     *
     * @psalm-impure
     */
    public function process(callable $callback): void;

    /**
     * @psalm-impure
     */
    public function isProcessing(): bool;

    /**
     * @psalm-impure
     */
    public function stopProcessing(): void;

    /**
     * Returns amount of processors can be different thread/process
     *
     * @psalm-impure
     */
    public function countProcessing(): int;

    /**
     * Returns amount of processable jobs
     *
     * @psalm-impure
     */
    public function countProcessable(): int;

    /**
     * @psalm-impure
     */
    public function delete(Identifier | Job $item): void;

    /**
     * @psalm-impure
     */
    public function bury(Job $job): void;

    /**
     * @psalm-impure
     */
    public function reanimate(Identifier $id, ?Timestamp $until = null): void;

    /**
     * @psalm-impure
     */
    public function getBuried(Paginate $paginate): Jobs;
}
