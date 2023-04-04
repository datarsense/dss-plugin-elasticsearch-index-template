package com.dataiku.dip.plugins.elasticsearch_utils;

import org.springframework.beans.factory.annotation.Autowired;

import com.dataiku.dip.cuspol.CustomPolicyHooks;
import com.dataiku.dip.security.AuthCtx;
import com.dataiku.dip.connections.DSSConnection;
import com.dataiku.dip.coremodel.Dataset;
import com.dataiku.dip.coremodel.Schema;
import com.dataiku.dip.coremodel.SerializedDataset;
import com.dataiku.dip.datalayer.utils.SchemaComparator;
import com.dataiku.dip.datasets.elasticsearch.ElasticSearchDatasetHandler;
import com.dataiku.dip.datasets.elasticsearch.ElasticSearchDialect;
import com.dataiku.dip.datasets.elasticsearch.ElasticSearchUtils;
import com.dataiku.dip.plugins.RegularPluginsRegistryService;
import com.dataiku.dip.server.services.TaggableObjectsService.TaggableObject;
import com.dataiku.dip.server.datasets.DatasetSaveService.DatasetCreationContext;
import com.dataiku.dip.utils.DKULogger;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.json.JSONObject;

import java.util.List;


public class elasticsearch_hooks extends CustomPolicyHooks {
    
    private static DKULogger logger = DKULogger.getLogger("dku.plugins.elasticsearch.hooks");
    
    @Autowired private RegularPluginsRegistryService regularPluginsRegistryService;

    @Override
    public void onPreObjectSave(AuthCtx user, TaggableObject before, TaggableObject after) throws Exception {
        if(after.getClass() == SerializedDataset.class && this.isElasticSearchDataset((SerializedDataset)after)) {
            // Get a list of columns defined in an ES index template
            JsonObject pluginSettings = regularPluginsRegistryService.getSettings("elasticsearch-index-template").config;

            boolean hasConnectionKey = pluginSettings != null && pluginSettings.has("es-connections");
            JsonArray connections = hasConnectionKey ? pluginSettings.get("es-connections").getAsJsonArray() : null;

            boolean hasColumnsKey = pluginSettings != null && pluginSettings.has("columns");
            String columns = hasColumnsKey ? pluginSettings.get("columns").getAsString() : null;
            String[] columnsList = columns.split(",");

            boolean hasCustomMappingAllowed = pluginSettings != null && pluginSettings.has("customMappingAllowed");
            Boolean customMappingAllowed = hasCustomMappingAllowed ? pluginSettings.get("customMappingAllowed").getAsBoolean() : null;

            SerializedDataset dsBefore = (SerializedDataset)before;
            SerializedDataset ds = (SerializedDataset)after;
            JsonElement dsConnectionElt = JsonParser.parseString(ds.getParams().getConnection());

            // If the columnsList contains at least one column name, check if dataset schema contains columns mapped in the ES index template
            if (connections.contains(dsConnectionElt.getAsJsonPrimitive()) && columnsList.length > 0) {
                
                // Get ES mappings configured for the dataset
                JSONObject inferedDatasetEsMappingObj = ElasticSearchUtils.getDefaultMappingDefinition(Dataset.fromSerialized(ds), ElasticSearchDialect.ES_7);
                
                String customMapping = ds.getParamsAs(ElasticSearchDatasetHandler.Config.class).customMapping;
                JSONObject customMappingObj = null;
                if(customMapping != null && !customMapping.trim().isEmpty() && isValidElasticsearchMapping(customMapping)) {
                    customMappingObj = (new JSONObject(customMapping));
                }

                // Check if dataset schema has been changed
                Schema afterSchema = ds.getSchema();
                Schema beforeSchema = dsBefore.getSchema();
                List<String> schemaDiff = SchemaComparator.findDifferences(beforeSchema, afterSchema, false);
                
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

    @Override
    public void onPreDatasetCreation(AuthCtx user, SerializedDataset serializedDataset, DatasetCreationContext context) throws Exception {
     // To be implemented

    }

    @Override
    public void onPreConnectionSave(AuthCtx user, DSSConnection before, DSSConnection after) throws Exception {
     // To be implemented
    }
}
