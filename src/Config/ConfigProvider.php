<?php

declare(strict_types=1);

namespace LesQueue\Config;

use LesQueue\Queue;
use LesQueue\DbalQueue;
use LesQueue\PgsqlQueue;
use LesQueue\RabbitMqQueue;
use LesQueue\Config\Factory\DbalQueueFactory;
use LesQueue\Config\Factory\PgsqlQueueFactory;
use LesQueue\Config\Factory\RabbitMqQueueFactory;

/**
 * @psalm-immutable
 */
final class ConfigProvider
{
    /**
     * @param class-string<Queue> $useQueue
     *
     * @psalm-pure
     */
    public function __construct(private readonly string $useQueue)
    {}

    /**
     * @return array<string, mixed>
     *
     * @psalm-mutation-free
     *
     * @psalm-suppress DeprecatedClass
     */
    public function __invoke(): array
    {
        return [
            'dependencies' => [
                'aliases' => [
                    Queue::class => $this->useQueue,
                ],
                'factories' => [
                    // @phpstan-ignore-next-line
                    RabbitMqQueue::class => RabbitMqQueueFactory::class,
                    DbalQueue::class => DbalQueueFactory::class,
                    PgsqlQueue::class => PgsqlQueueFactory::class,
                ],
            ],
        ];
    }
}
