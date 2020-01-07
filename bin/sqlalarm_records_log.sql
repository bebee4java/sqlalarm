DROP TABLE IF EXISTS `sqlalarm_records_log`;

-- 告警记录详情日志表
CREATE TABLE IF NOT EXISTS `sqlalarm_records_log` (
  `id` int(20) unsigned NOT NULL AUTO_INCREMENT,
  `job_id` varchar(128) NOT NULL COMMENT '作业id',
  `job_stat` varchar(128) NOT NULL COMMENT '作业状态',
  `event_time` timestamp NOT NULL COMMENT '作业事件时间',
  `message` varchar(2000) NOT NULL COMMENT '作业告警消息',
  `context` varchar(2000) NOT NULL COMMENT '作业上下文参数',
  `title` varchar(128) NOT NULL COMMENT '告警标题',
  `platform` varchar(128) NOT NULL COMMENT '告警平台',
  `item_id` varchar(128) NOT NULL COMMENT '告警项id',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`id`),
  KEY `job_index` (`job_id`, `job_stat`),
  KEY `alarm_item_index` (`platform`, `item_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '告警记录详情日志表';
