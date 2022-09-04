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
    public static function getMinLength(): int
    {
        return 4;
    }

    /**
     * @psalm-pure
     */
    public static function getMaxLength(): int
    {
        return 11;
    }
}
