<?php
declare(strict_types=1);

namespace LessQueue\Job;

use LessValueObject\Number\Int\Unsigned;

/**
 * @psalm-immutable
 */
interface Job
{
    public function getName(): Property\Name;

    public function getPriority(): Property\Priority;

    /**
     * @return array<mixed>
     */
    public function getData(): array;

    public function getAttempt(): Unsigned;
}
