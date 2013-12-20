/*
 * SiteWhereSolrFactory.java 
 * --------------------------------------------------------------------------------------
 * Copyright (c) Reveal Technologies, LLC. All rights reserved. http://www.reveal-tech.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package com.sitewhere.solr;

import java.util.List;

import org.apache.solr.common.SolrInputDocument;

import com.sitewhere.spi.SiteWhereException;
import com.sitewhere.spi.common.IMeasurementEntry;
import com.sitewhere.spi.common.IMetadataEntry;
import com.sitewhere.spi.device.IDeviceAlert;
import com.sitewhere.spi.device.IDeviceEvent;
import com.sitewhere.spi.device.IDeviceLocation;
import com.sitewhere.spi.device.IDeviceMeasurements;

/**
 * Factory that creates indexable Solr objects from SiteWhere objects.
 * 
 * @author Derek
 */
public class SiteWhereSolrFactory {

	/**
	 * Create a {@link SolrInputDocument} based on information in
	 * {@link IDeviceMeasurements}.
	 * 
	 * @param measurements
	 * @return
	 * @throws SiteWhereException
	 */
	public static SolrInputDocument createDocumentFromMeasurements(IDeviceMeasurements measurements)
			throws SiteWhereException {
		SolrInputDocument document = new SolrInputDocument();
		addFieldsForEvent(document, measurements);
		for (IMeasurementEntry entry : measurements.getMeasurements()) {
			document.addField(ISolrFields.MEASUREMENT_PREFIX + entry.getName(), entry.getValue());
		}
		return document;
	}

	/**
	 * Create a {@link SolrInputDocument} based on information in {@link IDeviceLocation}.
	 * 
	 * @param location
	 * @return
	 * @throws SiteWhereException
	 */
	public static SolrInputDocument createDocumentFromLocation(IDeviceLocation location)
			throws SiteWhereException {
		SolrInputDocument document = new SolrInputDocument();
		addFieldsForEvent(document, location);
		String latLong = "" + location.getLatitude() + ", " + location.getLongitude();
		document.addField(ISolrFields.LOCATION, latLong);
		document.addField(ISolrFields.ELEVATION, location.getElevation());
		return document;
	}

	/**
	 * Create a {@link SolrInputDocument} based on information in {@link IDeviceAlert}.
	 * 
	 * @param alert
	 * @return
	 * @throws SiteWhereException
	 */
	public static SolrInputDocument createDocumentFromAlert(IDeviceAlert alert) throws SiteWhereException {
		SolrInputDocument document = new SolrInputDocument();
		addFieldsForEvent(document, alert);
		document.addField(ISolrFields.ALERT_TYPE, alert.getType());
		document.addField(ISolrFields.ALERT_MESSAGE, alert.getMessage());
		document.addField(ISolrFields.ALERT_LEVEL, alert.getLevel().name());
		document.addField(ISolrFields.ALERT_SOURCE, alert.getSource().name());
		return document;
	}

	/**
	 * Adds common fields from base SiteWhere {@link IDeviceEvent} object.
	 * 
	 * @param document
	 * @param event
	 * @throws SiteWhereException
	 */
	protected static void addFieldsForEvent(SolrInputDocument document, IDeviceEvent event)
			throws SiteWhereException {
		document.addField(ISolrFields.EVENT_ID, event.getId());
		document.addField(ISolrFields.ASSIGNMENT_TOKEN, event.getDeviceAssignmentToken());
		document.addField(ISolrFields.SITE_TOKEN, event.getSiteToken());
		document.addField(ISolrFields.EVENT_DATE, event.getEventDate());
		document.addField(ISolrFields.RECEIVED_DATE, event.getReceivedDate());
		addMetadata(document, event.getMetadata());
	}

	/**
	 * Add a list of {@link IMetadataEntry} objects to a document.
	 * 
	 * @param document
	 * @param metadata
	 * @throws SiteWhereException
	 */
	protected static void addMetadata(SolrInputDocument document, List<IMetadataEntry> metadata)
			throws SiteWhereException {
		for (IMetadataEntry entry : metadata) {
			document.addField(ISolrFields.META_PREFIX + entry.getName(), entry.getValue());
		}
	}
}