package com.github.zhangchunsheng.flink.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Desc:
 * Created by Peter on 2021/09/01 11:00
 */

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MetricEvent {

	/**
	 * Metric name
	 */
	private String name;

	/**
	 * Metric timestamp
	 */
	private Long timestamp;

	/**
	 * Metric fields
	 */
	private Map<String, Object> fields;

	/**
	 * Metric tags
	 */
	private Map<String, String> tags;
}
