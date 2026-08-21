<?php

declare(strict_types=1);

namespace LesQueue\Config\Factory;

use LesQueue\RabbitMqQueue;
use Doctrine\DBAL\Connection;
use Psr\Container\ContainerInterface;
use Psr\Container\NotFoundExceptionInterface;
use Psr\Container\ContainerExceptionInterface;
use PhpAmqpLib\Connection\AMQPStreamConnection;

/**
 * @deprecated tied to RabbitMqQueue
 *
 * @psalm-suppress DeprecatedClass
 */
final class RabbitMqQueueFactory
{
    /**
     * @throws ContainerExceptionInterface
     * @throws NotFoundExceptionInterface
     */
    public function __invoke(ContainerInterface $container): RabbitMqQueue
    {
        $config = $container->get('config');
        assert(is_array($config));
        assert(is_array($config[AMQPStreamConnection::class]));

        assert(is_string($config[AMQPStreamConnection::class]['host']));
        assert(is_int($config[AMQPStreamConnection::class]['port']));
        assert(is_string($config[AMQPStreamConnection::class]['user']));
        assert(is_string($config[AMQPStreamConnection::class]['pass']));
        assert(is_string($config[AMQPStreamConnection::class]['vhost']));

        $heartbeat = $config[AMQPStreamConnection::class]['heartbeat'] ?? 30;
        assert(is_int($heartbeat));

        $connection = new AMQPStreamConnection(
            $config[AMQPStreamConnection::class]['host'],
            $config[AMQPStreamConnection::class]['port'],
            $config[AMQPStreamConnection::class]['user'],
            $config[AMQPStreamConnection::class]['pass'],
            $config[AMQPStreamConnection::class]['vhost'],
            heartbeat: $heartbeat,
        );

        $database = $container->get(Connection::class);
        assert($database instanceof Connection);

        return new RabbitMqQueue($connection, $database);
    }
}
