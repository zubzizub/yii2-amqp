<?php

namespace zubzizub\amqp;

use PhpAmqpLib\Message\AMQPMessage;
use yii\base\InvalidCallException;
use yii\console\Controller;
use yii\helpers\Inflector;

abstract class QueueListenerController extends Controller
{
    /**
     * @var AMQPManager
     */
    public $manager;

    /**AbstractQueueListenerController
     * @var string
     */
    protected $callbackAction;

    public $defaultAction = 'listen';

    /**
     * @param string $queueName
     * @throws \yii\base\InvalidConfigException
     */
    public function actionListen(string $queueName)
    {
        $this->manager = \Yii::createObject(AMQPManager::class);

        $this->manager->prepare($queueName);
        $this->setCallbackAction();
        $this->manager->listen([$this, 'callback']);
    }

    protected function setCallbackAction()
    {
        $routingKey = $this->manager->getQueue();
        $this->callbackAction = Inflector::camelize($routingKey) . 'Listener';
        if (!method_exists($this, $this->callbackAction)) {
            throw new InvalidCallException("Action \"{$this->callbackAction}\" for route \"{$routingKey}\" not found");
        }
    }

    public function getCallbackAction()
    {
        return $this->callbackAction;
    }

    public function callback(AMQPMessage $message)
    {
        $messageBody = json_decode($message->getBody(), true);
        try {
            $actionName = $this->getCallbackAction();
            $this->{$actionName}($messageBody);
            $message->delivery_info['channel']->basic_ack($message->delivery_info['delivery_tag']);

        } catch (\Exception $e) {
            /*$message->delivery_info['channel']->basic_ack($message->delivery_info['delivery_tag']);

            $log[] = "Listener exception: {$e->getMessage()}";
            $log[] = "Exception class: " . get_class($e);
            $log[] = "Message: {$message}";
            $log[] = "Trace: {$e->getTraceAsString()}";
            $log = implode(PHP_EOL, $log);

            \Yii::error($log);*/

        }
        gc_collect_cycles();
    }
}