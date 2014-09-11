package kgroup.kartifact;

import java.util.Map;

import kafka.javaapi.producer.Producer;

/**
* This abstract class forms the basis of all connectors
* 
* @author Kaab
* 
*/
public abstract class Connector {
	/** source location from where the data is to be copied **/


	private String source = null; // to be used by consumer for zookeeper address
	

	/** sink location where the data is to be stored **/
	private String sink = null;
	/** size for the file to be stored **/
	private String filter = null;
	/** size of the file to be created after merge **/
	private long chunkSize;
	/** whether the directory structure is to be maintained or not **/
	private boolean preserverDirStructure = false;
	/** hashmap to store any credentials **/
	private Map<String, Object> credentials;
	/** data preference status **/
	private byte preferenceStatus;
	/** final preference combinations **/
	protected final byte DEFAULT = 0;
	protected final byte COMPRESS_ONLY = 1;
	protected final byte MERGE_ONLY = 10;
	protected final byte COMPRESS_MERGE = 11;

	/**
	 * Authenticate credentials
	 * 
	 * @return the authentication success
	 */
	public abstract boolean authenticate();

	/**
	 * Fetch the data from source and write to sink
	 * 
	 * @param params Key value pairs pertaining info about data
	 * @return whether the data fetched and stored successfully
	 */
	public abstract Object fetch(Map<String, Object> params);

	/**
	 * Upload the data from sink to source
	 * 
	 * @param params Key value pairs pertaining info about data
	 * @return whether the data uploaded successfully
	 */
	public abstract boolean put(Map<String, Object> params);

	/**
	 * Sample the data
	 * 
	 * @return the sample data
	 */
	public abstract String getSample();

	/**
	 * Search data
	 * 
	 * @param searchFilter
	 *            Filter specifying search criteria
	 * @return the searched results
	 */
	public abstract String search(String searchFilter);

	/**
	 * Getters and setters
	 */
	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getSink() {
		return sink;
	}

	public void setSink(String sink) {
		this.sink = sink;
	}

	public String getFilter() {
		return filter;
	}

	public void setFilter(String filter) {
		this.filter = filter;
	}

	public long getChunkSize() {
		return chunkSize;
	}

	public void setChunkSize(long chunkSize) {
		this.chunkSize = chunkSize;
	}

	public byte getPreferenceStatus() {
		return preferenceStatus;
	}

	public void setPreferenceStatus(byte preferenceStatus) {
		this.preferenceStatus = preferenceStatus;
	}

	public boolean isPreserverDirStructure() {
		return preserverDirStructure;
	}

	public void setPreserverDirStructure(boolean preserverDirStructure) {
		this.preserverDirStructure = preserverDirStructure;
	}

	public Map<String, Object> getCredentials() {
		return credentials;
	}

	public void setCredentials(Map<String, Object> credentials) {
		this.credentials = credentials;
	}

	public Producer<String, String> getProducer(String string) {
		// TODO Auto-generated method stub
		return null;
	}
}

