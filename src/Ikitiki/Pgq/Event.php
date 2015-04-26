<?php

namespace Ikitiki\Pgq;

abstract class Event implements \JsonSerializable
{
    /**
     * Event Id
     *
     * @var int
     */
    private $id;

    /**
     * Event time
     *
     * @var string
     */
    private $time;

    /**
     * Transaction id
     *
     * @var int
     */
    private $txId;

    /**
     * Variables container
     *
     * @var array
     */
    private $data;

    /**
     * Fail reason
     *
     * @var string
     */
    private $failReason;

    /**
     * Do we need to put current event again to the queue
     *
     * @var bool
     */
    private $queueSelf = false;

    /**
     * Tick id
     *
     * @var integer
     */
    private $tickId;

    /**
     * Constructor
     *
     * @param array $data
     * @param int $id
     * @param int $txId
     * @param int $time
     * @param int $tickId
     */
    public function __construct(array $data, $id = null, $txId = null, $time = null, $tickId = null)
    {
        $this->id = $id;
        $this->txId = $txId;
        $this->time = $time;
        $this->data = $data;
        $this->tickId = $tickId;
    }

    /**
     * Get fail reason
     *
     * @return string
     */
    final public function getFailReason()
    {
        return $this->failReason;
    }

    /**
     * Set fail reason
     *
     * @param string $failReason
     */
    final protected function failed($failReason)
    {
        $this->failReason = $failReason;
    }

    /**
     * Put current event again to the queue
     */
    final protected function doSelfQueue()
    {
        $this->queueSelf = true;
    }

    /**
     * Do we need to put current event again to the queue
     *
     * @return bool
     */
    final public function needsSelfQueue()
    {
        return $this->queueSelf;
    }

    /**
     * Process the event
     *
     * @return mixed
     */
    abstract public function process();

    /**
     * Get event class
     *
     * @return mixed
     */
    final public function getType()
    {
        return static::class;
    }

    final public function __set($property, $value)
    {
        $this->data[$property] = $value;
    }

    final public function __get($property)
    {
        return isset($this->data[$property]) ? $this->data[$property] : null;
    }

    final public function __isset($property)
    {
        return isset($this->data[$property]);
    }

    /**
     * Data which are to serialize
     *
     * @link http://php.net/manual/en/jsonserializable.jsonserialize.php
     * @return mixed data for serialization
     */
    final public function jsonSerialize()
    {
        return $this->data;
    }
}
