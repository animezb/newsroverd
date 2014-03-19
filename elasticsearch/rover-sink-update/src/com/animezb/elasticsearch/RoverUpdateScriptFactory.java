package com.animezb.elasticsearch;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;

import com.animezb.elasticsearch.RoverUpdateScript;

import java.util.Map;

public class RoverUpdateScriptFactory implements NativeScriptFactory {

  @Override public ExecutableScript newScript (@Nullable Map<String,Object> params){
    return new RoverUpdateScript(params);
  }
}
