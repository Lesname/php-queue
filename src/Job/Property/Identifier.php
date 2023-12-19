<?php
declare(strict_types=1);

namespace LessQueue\Job\Property;

use LessValueObject\String\AbstractStringValueObject;

/**
 * @psalm-immutable
 */
final class Identifier extends AbstractStringValueObject
{
    /**
     * @psalm-pure
     */
    public static function getMinimumLength(): int
    {
        return 1;
    }

    /**
     * @psalm-pure
     */
    public static function getMaximumLength(): int
    {
        return 11;
    }
}
