package com.animezb.elasticsearch;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.AbstractExecutableScript;

import gnu.trove.map.hash.TObjectByteHashMap;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.lang.Math;
import java.io.ObjectOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.InputStream;


public class RoverUpdateScript extends AbstractExecutableScript {
	private Map<String, Object> params;
	private Map<String, Object> vars;

	public RoverUpdateScript(@Nullable Map<String,Object> params){
		this.params = params;
		this.vars = new HashMap<String, Object>();
	}

	private static boolean isInteger(String s) {
		if(s.isEmpty()) return false;
		for(int i = 0; i < s.length(); i++) {
			if(i == 0 && s.charAt(i) == '-') {
				if(s.length() == 1) return false;
				else continue;
			}
			if(Character.digit(s.charAt(i),10) < 0) return false;
		}
		return true;
	}

	private static String GetFileExtention(String fileName) {
		String extension = "";
		fileName = fileName.toLowerCase();
		int i = fileName.lastIndexOf('.');
		if (i >= 0) {
			extension = fileName.substring(i+1);
		}
		if (isInteger(extension)) {
			return "split";
		}
		return extension;
	}

	private static String SegmentId(Map segment) {
		return String.format("%s%d", segment.get("filename"), segment.get("part"));
	}

	private Object runFile() throws java.io.IOException, java.io.UnsupportedEncodingException, java.lang.ClassNotFoundException {
		if (vars.containsKey("ctx") && vars.get("ctx") instanceof Map) {
			Map ctx = (Map) vars.get("ctx");
			if (ctx.containsKey("_source") && ctx.get("_source") instanceof Map) {
				Map source = (Map) ctx.get("_source");
				if (!source.containsKey("subject")) {
					source.put("subject", params.get("subject"));
				}
				if (!source.containsKey("filename")) {
					source.put("filename", params.get("filename"));
				}
				if (!source.containsKey("poster")) {
					source.put("poster", params.get("poster"));
				}
				int length;
				if (!source.containsKey("length") || !(source.get("length") instanceof Number)) {
					source.put("length", params.get("length"));
					length = ((Number) params.get("length")).intValue();
				} else {
					length = ((Number) source.get("length")).intValue();
				}
				TObjectByteHashMap<String> smap;
				if (source.containsKey("segments")) {
					byte[] segments = ((String)source.get("segments")).getBytes("UTF-8");
					segments = org.apache.commons.codec.binary.Base64.decodeBase64(segments);
					ByteArrayInputStream bis = new ByteArrayInputStream(segments);
					ObjectInputStream in = new ObjectInputStream(bis);
					smap = (TObjectByteHashMap<String>) in.readObject();
					in.close();
					bis.close();
				} else {
					smap = new TObjectByteHashMap<String>();
				}

				int complete = 0;
				long size = 0;
				if (source.containsKey("complete") && source.get("complete") instanceof Number) {
					complete = ((Number)source.get("complete")).intValue();
				}
				if (source.containsKey("size") && source.get("size") instanceof Number) {
					size = ((Number)source.get("size")).longValue();
				}

				String date = (String) source.get("date");

				if (params.containsKey("segments") && params.get("segments") instanceof List) {
					List segments = (List) params.get("segments");
					for (Object element : segments) {
						if (element instanceof Map) {
							Map segment = (Map) element;
							if (!smap.containsKey(segment.get("part").toString())) {
								if (date == null || (segment.containsKey("date") && (date.compareTo((String) segment.get("date")) < 0))) {
									date = (String) segment.get("date");
								}
								long ssz = 0;
								if (segment.containsKey("bytes") && segment.get("bytes") instanceof Number) {
									ssz = ((Number)segment.get("bytes")).longValue();
								}
								size += ssz;
								complete += 1;
								smap.put(segment.get("part").toString(), (byte)1);
							}
						}
					}
				}

				List groups;
				if (source.containsKey("group") && source.get("group") instanceof List) {
					groups = (List) source.get("group");
				} else {
					groups = new ArrayList();
				}

				if (params.containsKey("group") && params.get("group") instanceof List) {
					List sg = (List) params.get("group");
					for (Object element : sg) {
						if (element instanceof String) {
							String group = (String) element;
							if (!groups.contains(group)) {
								groups.add(group);
							}
						}
					}
				}

				source.put("date", date);
				//source.put("length", length);
				source.put("size", size);
				source.put("complete", complete);
				if (length == 0) {
					source.put("completion", 0);
				} else {
					source.put("completion", ((double)complete) / ((double)length));
				}
				source.put("group", groups);

				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream out = new ObjectOutputStream(bos);
				out.writeObject(smap);
				out.close();
				byte[] data = bos.toByteArray();
				source.put("segments", new String(org.apache.commons.codec.binary.Base64.encodeBase64(data), "UTF-8"));
				bos.close();
			}
		}
		return null;
	}

	private Object runUpload() throws java.io.IOException, java.io.UnsupportedEncodingException, java.lang.ClassNotFoundException {
		if (vars.containsKey("ctx") && vars.get("ctx") instanceof Map) {
			Map ctx = (Map) vars.get("ctx");
			if (ctx.containsKey("_source") && ctx.get("_source") instanceof Map) {
				Map source = (Map) ctx.get("_source");
				if (!source.containsKey("subject")) {
					source.put("subject", params.get("subject"));
				}
				if (!source.containsKey("poster")) {
					source.put("poster", params.get("poster"));
				}
				TObjectByteHashMap<String> smap;
				if (source.containsKey("segments")) {
					byte[] segments = ((String)source.get("segments")).getBytes("UTF-8");
					segments = org.apache.commons.codec.binary.Base64.decodeBase64(segments);
					ByteArrayInputStream bis = new ByteArrayInputStream(segments);
					ObjectInputStream in = new ObjectInputStream(bis);
					smap = (TObjectByteHashMap<String>) in.readObject();
					in.close();
					bis.close();
				} else {
					smap = new TObjectByteHashMap<String>();
				}

				int complete = 0;
				long size = 0;
				int length = 0;
				if (source.containsKey("complete") && source.get("complete") instanceof Number) {
					complete = ((Number)source.get("complete")).intValue();
				}
				if (source.containsKey("size") && source.get("size") instanceof Number) {
					size = ((Number)source.get("size")).longValue();
				}
				if (source.containsKey("length") && source.get("length") instanceof Number) {
					length = ((Number)source.get("length")).intValue();
				}

				String date = (String) source.get("date");
				String filePrefix = (String) source.get("fileprefix");
				Map fileTypes;
				if (source.containsKey("types") && source.get("types") instanceof Map) {
					fileTypes = (Map) source.get("types");
				} else {
					fileTypes = new HashMap<String, Number>();
				}

				if (params.containsKey("segments") && params.get("segments") instanceof List) {
					List segments = (List) params.get("segments");
					for (Object element : segments) {
						if (element instanceof Map) {
							Map segment = (Map) element;
							if (!smap.containsKey(SegmentId(segment))) {
								if (date == null || (segment.containsKey("date") && (date.compareTo((String) segment.get("date")) < 0))) {
									date = (String) segment.get("date");
								}
								long ssz = 0;
								if (segment.containsKey("bytes") && segment.get("bytes") instanceof Number) {
									ssz = ((Number)segment.get("bytes")).longValue();
								}
								size += ssz;
								complete += 1;
								smap.put(SegmentId(segment), (byte)1);
							}
							if (!smap.containsKey(segment.get("filename"))) {
								String fn = (String) segment.get("filename");
								if (filePrefix == null) {
									filePrefix = fn;
								} else {
									int j;
									for(j = 0; j < Math.min(filePrefix.length(), fn.length()); ++j) {
										if(filePrefix.charAt(j) != fn.charAt(j)) {
											break;
										}
									}
									filePrefix = filePrefix.substring(0, j);
								}
								String ext = GetFileExtention(fn);
								if (ext != "") {
									int n = 1;
									Object ov = fileTypes.get(ext);
									if (ov != null) {
										n += ((Number)ov).intValue();
									}
									fileTypes.put(ext, n);
								}
								if (segment.containsKey("length") && segment.get("length") instanceof Number) {
									length += ((Number)segment.get("length")).intValue();
								}
								smap.put((String) segment.get("filename"), (byte)1);
							}
						}
					}
				}

				List groups;
				if (source.containsKey("group") && source.get("group") instanceof List) {
					groups = (List) source.get("group");
				} else {
					groups = new ArrayList();
				}

				if (params.containsKey("group") && params.get("group") instanceof List) {
					List sg = (List) params.get("group");
					for (Object element : sg) {
						if (element instanceof String) {
							String group = (String) element;
							if (!groups.contains(group)) {
								groups.add(group);
							}
						}
					}
				}

				source.put("date", date);
				source.put("fileprefix", filePrefix);
				source.put("length", length);
				source.put("size", size);
				source.put("complete", complete);
				if (length == 0) {
					source.put("completion", 0);
				} else {
					source.put("completion", ((double)complete) / ((double)length));
				}
				source.put("group", groups);
				source.put("types", fileTypes);

				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream out = new ObjectOutputStream(bos);
				out.writeObject(smap);
				out.close();
				byte[] data = bos.toByteArray();
				source.put("segments", new String(org.apache.commons.codec.binary.Base64.encodeBase64(data), "UTF-8"));
				bos.close();
			}
		}
		return null;
	}

	@Override
	public Object run() {
		if (params.containsKey("type")) {
			try {
				if (params.get("type").equals("upload")) {
					return runUpload();
				} else if (params.get("type").equals("file")) {
					return runFile();
				}
			} catch (java.io.UnsupportedEncodingException exception) {
				System.out.println("An exception occured: " + exception.getMessage());
			} catch (java.io.IOException exception) {
				System.out.println("An exception occured: " + exception.getMessage());
			} catch (java.lang.ClassNotFoundException exception) {
				System.out.println("An exception occured: " + exception.getMessage());
			}
		}
		return null;
	}

	@Override
	public void setNextVar(String name, Object value) {
		vars.put(name, value);
	}
}