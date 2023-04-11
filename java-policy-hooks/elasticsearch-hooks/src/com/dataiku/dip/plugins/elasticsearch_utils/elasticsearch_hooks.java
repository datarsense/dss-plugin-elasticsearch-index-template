package com.dataiku.dip.plugins.elasticsearch_utils;

import org.springframework.beans.factory.annotation.Autowired;

import com.dataiku.dip.cuspol.CustomPolicyHooks;
import com.dataiku.dip.security.AuthCtx;
import com.dataiku.dip.connections.DSSConnection;
import com.dataiku.dip.coremodel.Dataset;
import com.dataiku.dip.coremodel.InfoMessage.FixabilityCategory;
import com.dataiku.dip.coremodel.InfoMessage.MessageCode;
import com.dataiku.dip.coremodel.Schema;
import com.dataiku.dip.coremodel.SerializedDataset;
import com.dataiku.dip.datalayer.utils.SchemaComparator;
import com.dataiku.dip.datasets.elasticsearch.ElasticSearchDatasetHandler;
import com.dataiku.dip.datasets.elasticsearch.ElasticSearchDialect;
import com.dataiku.dip.datasets.elasticsearch.ElasticSearchUtils;
import com.dataiku.dip.exceptions.CodedException;
import com.dataiku.dip.plugins.presets.PluginPreset;
import com.dataiku.dip.plugins.RegularPluginsRegistryService;
import com.dataiku.dip.server.services.TaggableObjectsService.TaggableObject;
import com.dataiku.dip.server.datasets.DatasetSaveService.DatasetCreationContext;
import com.dataiku.dip.utils.DKULogger;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class elasticsearch_hooks extends CustomPolicyHooks {
    
    private static DKULogger logger = DKULogger.getLogger("dku.plugins.elasticsearch.hooks");

    private static MessageCode mc = new MessageCode() {
        @Override
        public String getCode() {
            return "ERR_ES_DATASET_CREATION";
        }

        @Override
        public String getCodeTitle() {
            return "Cannot create dataset";
        }

        @Override
        public FixabilityCategory getFixability() {
            return FixabilityCategory.ADMIN_SETTINGS_PLUGINS;
        }
    };
    
    @Autowired private RegularPluginsRegistryService regularPluginsRegistryService;

    @Override
    public void onPreObjectSave(AuthCtx user, TaggableObject before, TaggableObject after) throws Exception {
        if(after instanceof SerializedDataset 
                && this.isElasticSearchDataset((SerializedDataset)after)
                && ((SerializedDataset)after).managed) {

            SerializedDataset dsBefore = (SerializedDataset)before;
            SerializedDataset ds = (SerializedDataset)after;
            
            // Get a list of columns defined in an ES index template
            Boolean allColumns = checkRemoveAllColumns(ds);
            
            String[] columnsList = getColumnsListToRemove(ds);
            
            JsonObject pluginSettings = regularPluginsRegistryService.getSettings("elasticsearch-index-template").config;
            boolean hasCustomMappingAllowed = pluginSettings != null && pluginSettings.has("customMappingAllowed");
            Boolean customMappingAllowed = hasCustomMappingAllowed ? pluginSettings.get("customMappingAllowed").getAsBoolean() : null;
            
            // If all columns are defined in an Elasticsearh index template, write an empty custom mapping
            if (allColumns) {
                JSONObject customDatasetEsMapping = new JSONObject();
                customDatasetEsMapping.put("properties", new JSONObject());
                ds.getParamsAs(ElasticSearchDatasetHandler.Config.class).customMapping = customDatasetEsMapping.toString(4);
                logger.info((Object)("Configuring custom elasticsearch mapping " + ((ElasticSearchDatasetHandler.Config)ds.getParams()).customMapping) + " for dataset " + ds.name);

            }
            // Else if the columnsList contains at least one column name, check if dataset schema contains columns mapped in the ES index template
            else if (columnsList.length > 0) {
                // Get ES mappings configured for the dataset
                JSONObject inferedDatasetEsMappingObj = ElasticSearchUtils.getDefaultMappingDefinition(Dataset.fromSerialized(ds), ElasticSearchDialect.ES_7);
                
                String customMapping = ds.getParamsAs(ElasticSearchDatasetHandler.Config.class).customMapping;
                JSONObject customMappingObj = null;
                if(customMapping != null && !customMapping.trim().isEmpty() && isValidElasticsearchMapping(customMapping)) {
                    customMappingObj = (new JSONObject(customMapping));
                }

                // Check if dataset schema has been changed
                Schema afterSchema = ds.getSchema();

                List<String> schemaDiff = new ArrayList<>();
                if (dsBefore != null) {
                    Schema beforeSchema = dsBefore.getSchema();
                    schemaDiff = SchemaComparator.findDifferences(beforeSchema, afterSchema, false);
                }
                
                // Use custom mapping if it exists and if dataset schema has not been modified 
                // to allow field type mapping customization for ES type not implemented in DSS
                // Otherwise : Overwrite custom mapping with inferedDatasetMapping
                JSONObject sourceMappingObj = (customMappingAllowed && schemaDiff.isEmpty() && customMappingObj != null ) ? customMappingObj : inferedDatasetEsMappingObj;
                JSONObject sourceMappingPropertiesObj = sourceMappingObj.getJSONObject("properties");

                // A custom ES mapping will only be required if the dataset contains columns mapped in an index template.
                boolean customEsMappingRequired = false;

                // For each column defined in the index template test if dataset schema contains it,
                // remove it from the dataset ES index mapping, and flag custom ES mapping as required.
                for (String col : columnsList) {
                    if(sourceMappingPropertiesObj.has(col)) {
                        sourceMappingPropertiesObj.remove(col);
                        customEsMappingRequired = true;
                    }
                }  
                 
                // Build the custom Elasticsearch JSON mapping if required and write it to the Elasticsearch connection parameters of the dataset
                if (customEsMappingRequired) {
                    JSONObject customDatasetEsMapping = new JSONObject();
                    customDatasetEsMapping.put("properties", sourceMappingPropertiesObj);
                    ds.getParamsAs(ElasticSearchDatasetHandler.Config.class).customMapping = customDatasetEsMapping.toString(4);
                    logger.info((Object)("Configuring custom elasticsearch mapping " + ((ElasticSearchDatasetHandler.Config)ds.getParams()).customMapping) + " for dataset " + ds.name);
                }
            }
        }
    }
    
    private boolean isElasticSearchDataset(SerializedDataset ds) {
        return ds.type.equals("ElasticSearch");
    }

    private boolean isValidElasticsearchMapping(String json) {
        try {
            JsonObject esMappingObj = JsonParser.parseString(json).getAsJsonObject();
            return esMappingObj.has("properties");
        } catch (Exception e) {
            return false;
        }
    }

    private Boolean checkRemoveAllColumns(SerializedDataset ds) throws CodedException,IOException {
        JsonElement dsConnectionElt = JsonParser.parseString(ds.getParams().getConnection());
        Boolean connectionFound = false;
        Boolean result = false;
        List<PluginPreset> presets = regularPluginsRegistryService.getSettings("elasticsearch-index-template").presets;

        // Find if dataset connection has been defined in a plugin preset
        // If yes, check if all columns have to be hidden
        for (PluginPreset p : presets) {
            if (p.pluginConfig.has("es-connections") && p.pluginConfig.has("allColumns")) {
                JsonArray connections = p.pluginConfig.get("es-connections").getAsJsonArray(); 
                logger.info((Object)("DEBUGCODE " + p.pluginConfig.toString()));

                // Throw an error if ES connection if found in more than one preset
                if(connectionFound && connections.contains(dsConnectionElt.getAsJsonPrimitive())) {
                    throw new CodedException(mc, "Connection " + dsConnectionElt.getAsJsonPrimitive().getAsString() + " is defined in multiple elasticsearch-index-template plugin presets. Unable to define which preset has to be used. Please contact a DSS administrator.");
                }

                // Extract columns to hide for the first connection in presets matching ES connection name
                if(!connectionFound && connections.contains(dsConnectionElt.getAsJsonPrimitive())) {
                    connectionFound = true;
                    result = p.pluginConfig.get("allColumns").getAsBoolean();
                }
            }
        }
        return result;
    }

    private String[] getColumnsListToRemove(SerializedDataset ds) throws CodedException,IOException {
        JsonElement dsConnectionElt = JsonParser.parseString(ds.getParams().getConnection());
        Boolean connectionFound = false;
        String[] columnsList = new String[0];
        List<PluginPreset> presets = regularPluginsRegistryService.getSettings("elasticsearch-index-template").presets;

        // Find if dataset connection has been defined in a plugin preset
        // If yes, extract the name of columns which have to be hidden
        for (PluginPreset p : presets) {
            if (p.pluginConfig.has("es-connections") && p.pluginConfig.has("columns")) {
                JsonArray connections = p.pluginConfig.get("es-connections").getAsJsonArray(); 
                logger.info((Object)("DEBUGCODE " + p.pluginConfig.toString()));

                // Throw an error if ES connection if found in more than one preset
                if(connectionFound && connections.contains(dsConnectionElt.getAsJsonPrimitive())) {
                    throw new CodedException(mc, "Connection " + dsConnectionElt.getAsJsonPrimitive().getAsString() + " is defined in multiple elasticsearch-index-template plugin presets. Unable to define which preset has to be used. Please contact a DSS administrator.");
                }

                // Extract columns to hide for the first connection in presets matching ES connection name
                if(!connectionFound && connections.contains(dsConnectionElt.getAsJsonPrimitive())) {
                    connectionFound = true;
                    String columns = p.pluginConfig.get("columns").getAsString();
                    columnsList = columns.length() > 0 ? columns.split(",") : columnsList;
                }
            }
        }
        return columnsList;
    }

    @Override
    public void onPreDatasetCreation(AuthCtx user, SerializedDataset serializedDataset, DatasetCreationContext context) throws Exception {
     // To be implemented

    }

    @Override
    public void onPreConnectionSave(AuthCtx user, DSSConnection before, DSSConnection after) throws Exception {
     // To be implemented
    }
}
