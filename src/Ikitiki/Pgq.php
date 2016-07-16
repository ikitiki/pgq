<?php

namespace Ikitiki;

/**
 * PGQ Consumer
 */
class Pgq
{
    /**
     * OK status in finish_batch
     */
    const STATUS_OK = 1;

    /**
     * Queue name
     *
     * @var string
     */
    private $queueName;

    /**
     * Consumer name
     *
     * @var string
     */
    private $consumerName;

    /**
     * DB
     *
     * @var DB
     */
    private $db;

    /**
     * Sleep interval
     *
     * @var int
     */
    private $sleepInterval = 0;

    /**
     * Filter upon ev_extra1 event field
     *
     * @var array
     */
    private $extra1Filter = [];

    /**
     * Batch events stats
     *
     * @var array
     */
    private $batchEventsStats = ['total' => 0, 'failed' => 0];

    public function __construct(DB $db)
    {
        $this->db = $db;
    }

    /**
     * Set queue name
     *
     * @param string $queueName
     */
    public function setQueueName($queueName)
    {
        $this->queueName = $queueName;
    }

    /**
     * Set consumer name
     *
     * @param string $consumerName
     */
    public function setConsumerName($consumerName)
    {
        $this->consumerName = $consumerName;
    }

    /**
     * Set sleep interval
     *
     * @param int $value
     */
    public function setSleepInterval($value)
    {
        $this->sleepInterval = $value;
    }

    /**
     * Set filter on extra1 field
     *
     * @param array $filter
     */
    public function setExtra1Filter(array $filter)
    {
        $this->extra1Filter = $filter;
    }

    /**
     * Get queue info
     *
     * @param null|string $queueName
     *
     * @return array
     */
    public function getQueueInfo($queueName = null)
    {
        $queueName = $queueName ?: $this->queueName;

        $sql =<<<SQL
select
    queue_name,
    queue_ntables,
    queue_cur_table,
    queue_rotation_period,
    queue_switch_time,
    queue_external_ticker,
    queue_ticker_max_count,
    queue_ticker_max_lag,
    queue_ticker_idle_period
from
  pgq.get_queue_info('%s')
SQL;

        return $this->db->execOne($sql, DB::quote($queueName));
    }

    /**
     * Get consumer info
     *
     * @param null|string $queueName
     * @param null|string $consumerName
     *
     * @return array|null
     */
    public function getConsumerInfo($queueName = null, $consumerName = null)
    {
        $queueName = $queueName ?: $this->queueName;
        $consumerName = $consumerName ?: $this->consumerName;

        $sql =<<<SQL
select
  queue_name,
  consumer_name,
  lag,
  last_seen,
  last_tick,
  current_batch,
  next_tick
from
 pgq.get_consumer_info('%s', '%s')
SQL;

        return $this->db->execOne($sql, DB::quote($queueName), DB::quote($consumerName));
    }

    /**
     * Subscribe consumer to the queue
     *
     * @param null|string $consumerName
     * @param null|string $queueName
     */
    public function subscribeConsumer($consumerName = null, $queueName = null)
    {
        $queueName = $queueName ?: $this->queueName;
        $consumerName = $consumerName ?: $this->consumerName;

        if ($this->getConsumerInfo($queueName, $consumerName) !== null) {
            return;
        }

        $this->db->exec(
            "select pgq.register_consumer('%s', '%s')",
            $queueName,
            $consumerName
        );
    }

    /**
     * Unsubscribe consumer from the queue
     *
     * @param null|string $consumerName
     * @param null|string $queueName
     */
    public function unsubscribeConsumer($consumerName = null, $queueName = null)
    {
        $queueName = $queueName ?: $this->queueName;
        $consumerName = $consumerName ?: $this->consumerName;

        if ($this->getConsumerInfo($queueName, $consumerName) === null) {
            return;
        }

        $this->db->exec(
            "select pgq.unregister_consumer('%s', '%s')",
            $queueName,
            $consumerName
        );
    }

    private function sleep()
    {
        if (!$this->sleepInterval) {
            return;
        }

        sleep($this->sleepInterval);
    }

    /**
     * Process batch
     *
     * @return bool
     * @throws \Exception
     */
    public function processBatch()
    {
        $batch = $this->db->execOne(
            "select n.n as id, i.tick_id from pgq.next_batch('%s', '%s') n, pgq.get_batch_info(n.n) i",
            DB::quote($this->queueName),
            DB::quote($this->consumerName)
        );

        if ($batch['id'] === null) {
            $this->sleep();
            return false;
        }

        $failedEventCnt = 0;
        $batchFailed = false;
        $events = $this->db->exec(
            'select
                ev_id, ev_time, ev_txid, ev_retry, ev_type, ev_data,
                ev_extra1, ev_extra2, ev_extra3, ev_extra4
             from pgq.get_batch_events(%d)',
            $batch['id']
        );

        $this->db->beginTransaction();
        foreach ($events as $eventData) {
            if (empty($eventData['ev_type'])) {
                continue;
            }

            if ($this->extra1Filter
                && $eventData['ev_extra1']
                && !in_array($eventData['ev_extra1'], $this->extra1Filter)
            ) {
                continue;
            }

            try {
                $event = $this->eventHandler($eventData, $batch['tick_id']);
                $event->process();
            } catch (\Exception $e) {
                $batchFailed = true;
                break;
            }

            if ($failReason = $event->getFailReason()) {
                $failedEventCnt++;
            }

            if ($event->needsSelfQueue()) {
                $this->queueEvent($event);
            }
        }

        if (!$batchFailed) {
            $status = $this->db->exec('select pgq.finish_batch(%d) as status', $batch['id'])->fetchField('status');
            if ($status != self::STATUS_OK) {
                $this->db->rollback();
                throw new \Exception('can`t finish batch');
            }
            $this->db->commit();
        } else {
            $this->db->rollback();
        }

        $this->batchEventsStats['total'] = count($events);
        $this->batchEventsStats['failed'] = $failedEventCnt;

        return true;
    }

    /**
     * Queue event
     *
     * @param PGQ\Event $event
     *
     * @return int
     */
    public function queueEvent(PGQ\Event $event)
    {
        return $this->db->exec(
            "select pgq.insert_event('%s', '%s', '%s') as event_id",
            DB::quote($this->queueName),
            DB::quote($event->getType()),
            DB::quote(json_encode($event, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES))
        )->fetchField('event_id');
    }

    /**
     * Get instance of event processor
     *
     * @param array $eventData
     * @param integer $tickId
     *
     * @return PGQ\Event
     * @throws \Exception
     */
    public function eventHandler(array $eventData, $tickId)
    {
        $className = $eventData['ev_type'];

        if (!class_exists($className)) {
            throw new \Exception(
                sprintf(
                    'Consumer class [%s] is not found',
                    $className
                )
            );
        }

        if (!is_subclass_of($className, Pgq\Event::class)) {
            throw new \Exception(
                sprintf(
                    'Consumer class [%s] does not implement Pgq_Event',
                    $className
                )
            );
        }

        $data = json_decode($eventData['ev_data'], true);

        return new $className(
            is_array($data) ? $data : [],
            $eventData['ev_id'],
            $eventData['ev_txid'],
            $eventData['ev_time'],
            $tickId
        );
    }

    /**
     * Batch stats
     *
     * @return array
     */
    public function batchStats()
    {
        return $this->batchEventsStats;
    }
}
