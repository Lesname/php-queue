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

interface Queue
{
    public function publish(Name $name, DynamicCompositeValueObject $data, ?Timestamp $until = null, ?Priority $priority = null): void;

    public function republish(Job $job, Timestamp $until, ?Priority $priority = null): void;

    /**
     * @param callable(Job $job): void $callback
     */
    public function process(callable $callback): void;

    public function isProcessing(): bool;

    public function stopProcessing(): void;

    /**
     * Returns amount of processors can be different thread/process
     */
    public function countProcessing(): int;

    /**
     * Returns amount of processable jobs
     */
    public function countProcessable(): int;

    public function delete(Identifier | Job $item): void;

    public function bury(Job $job): void;

    public function reanimate(Identifier $id, ?Timestamp $until = null): void;

    public function getBuried(Paginate $paginate): Jobs;
}
