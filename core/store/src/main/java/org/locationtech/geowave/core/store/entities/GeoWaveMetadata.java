/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.core.store.entities;

import java.util.Arrays;

public class GeoWaveMetadata
{
	private final byte[] primaryId;
	private final byte[] secondaryId;
	private final byte[] visibility;
	private final byte[] value;

	public GeoWaveMetadata(
			final byte[] primaryId,
			final byte[] secondaryId,
			final byte[] visibility,
			final byte[] value ) {
		this.primaryId = primaryId;
		this.secondaryId = secondaryId;
		this.visibility = visibility;
		this.value = value;
	}

	public byte[] getPrimaryId() {
		return primaryId;
	}

	public byte[] getSecondaryId() {
		return secondaryId;
	}

	public byte[] getVisibility() {
		return visibility;
	}

	public byte[] getValue() {
		return value;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + Arrays
				.hashCode(
						primaryId);
		result = (prime * result) + Arrays
				.hashCode(
						secondaryId);
		result = (prime * result) + Arrays
				.hashCode(
						value);
		result = (prime * result) + Arrays
				.hashCode(
						visibility);
		return result;
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final GeoWaveMetadata other = (GeoWaveMetadata) obj;
		if (!Arrays
				.equals(
						primaryId,
						other.primaryId)) {
			return false;
		}
		if (!Arrays
				.equals(
						secondaryId,
						other.secondaryId)) {
			return false;
		}
		if (!Arrays
				.equals(
						value,
						other.value)) {
			return false;
		}
		if (!Arrays
				.equals(
						visibility,
						other.visibility)) {
			return false;
		}
		return true;
	}
}
