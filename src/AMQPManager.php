<?php

namespace zubzizub\amqp;

use InvalidArgumentException;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use yii\helpers\Console;

class AMQPManager
{
    static $connection;
    public $host;
    public $port;
    public $vhost;
    public $login;
    public $password;
    public $queue;
    public $queues;

    protected $exchange;
    protected $message;

    /**
     * @var AMQPChannel
     */
    protected $channel;

    public function getConnection(): AMQPStreamConnection
    {
        if (!static::$connection) {
            $connection = new AMQPStreamConnection(
                $this->host,
                $this->port,
                $this->login,
                $this->password,
                $this->vhost
            );
            static::$connection = $connection;
        }
        return static::$connection;
    }

    public function getChannel(): AMQPChannel
    {
        if (!$this->channel) {
            $this->channel = ($this->getConnection())->channel();
            $this->channel->exchange_declare($this->exchange, 'direct', false, true, false);
            $this->channel->queue_declare($this->queue, false, true, false, false);
            $this->channel->queue_bind($this->queue, $this->exchange, $this->queue);
        }
        return $this->channel;
    }

    public function prepare(string $queueName): self
    {
        $this->setQueue($queueName);
        $this->setExchange($queueName);
        $this->getConnection();
        $this->getChannel();
        return $this;
    }

    public function push($message)
    {
        $messageBody = json_encode($message);
        $message = new AMQPMessage($messageBody, [
            'content_type' => 'application/json',
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
        ]);

        $this->channel->basic_publish($message, $this->exchange, $this->queue);
        $this->channel->close();
        $this->getConnection()->close();
    }

    public function listen(callable $callback)
    {
        Console::output("Start listener on queue \"{$this->queue}\"");

        $this->channel->basic_consume(
            $this->queue, '', false, false, false, false, $callback
        );

        while (count($this->channel->callbacks)) {
            $this->channel->wait();
        }

        $this->channel->close();
        $this->getConnection()->close();
    }

    public function setExchange(string $queue)
    {
        if (!isset($this->queues[$queue])) {
            throw new InvalidArgumentException('Queue "' . $queue . '" not found in configuration');
        }

        $this->exchange = $this->queues[$queue]['exchangeName'];
    }

    public function setQueue(string $queue)
    {
        if (!isset($this->queues[$queue])) {
            throw new InvalidArgumentException('Queue "' . $queue . '" not found in configuration');
        }

        $this->queue = $queue;
    }

    public function setMessage(array $message): self
    {
        $this->message = $message;
        return $this;
    }

    public function getQueue()
    {
        return $this->queue;
    }
}