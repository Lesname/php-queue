<?php
declare(strict_types=1);

namespace LesQueue\Job\Property;

use Override;
use LesValueObject\String\AbstractStringValueObject;

/**
 * @psalm-immutable
 */
final class Identifier extends AbstractStringValueObject
{
    /**
     * @psalm-pure
     */
    #[Override]
    public static function getMinimumLength(): int
    {
        return 1;
    }

    /**
     * @psalm-pure
     */
    #[Override]
    public static function getMaximumLength(): int
    {
        return 11;
    }
}
