<?php
declare(strict_types=1);

namespace LessQueue\Job;

use LessValueObject\Composite\AbstractCompositeValueObject;

/**
 * @psalm-immutable
 */
final class Job extends AbstractCompositeValueObject
{
    /**
     * @param string $id
     * @param Property\Name $name
     * @param Property\Priority $priority
     * @param array<mixed> $data
     */
    public function __construct(
        public readonly string $id,
        public readonly Property\Name $name,
        public readonly Property\Priority $priority,
        public readonly array $data,
    ) {}
}
