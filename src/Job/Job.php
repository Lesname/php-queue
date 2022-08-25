<?php
declare(strict_types=1);

namespace LessQueue\Job;

use LessValueObject\Composite\AbstractCompositeValueObject;
use LessValueObject\Number\Int\Unsigned;

/**
 * @psalm-immutable
 */
final class Job extends AbstractCompositeValueObject
{
    /**
     * @param array<mixed> $data
     */
    public function __construct(
        public readonly Property\Identifier $id,
        public readonly Property\Name $name,
        public readonly array $data,
        public readonly Unsigned $attempt,
    ) {}
}
