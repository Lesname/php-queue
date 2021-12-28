<?php
declare(strict_types=1);

namespace LessQueue\Job\Property;

use LessValueObject\String\Format\AbstractRegexpFormattedStringValueObject;

/**
 * @psalm-immutable
 */
final class Name extends AbstractRegexpFormattedStringValueObject
{
    /**
     * @psalm-pure
     */
    public static function getRegexPattern(): string
    {
        return '^[a-z][a-zA-Z]*(\.[a-z][a-zA-Z]*)*:[a-z][a-zA-Z]*$';
    }

    /**
     * @psalm-pure
     */
    public static function getMinLength(): int
    {
        return 3;
    }

    /**
     * @psalm-pure
     */
    public static function getMaxLength(): int
    {
        return 50;
    }
}
