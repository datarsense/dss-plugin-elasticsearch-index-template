package com.dataiku.dip.plugins.elasticsearch_utils;

import org.springframework.beans.factory.annotation.Autowired;

import com.dataiku.dip.cuspol.CustomPolicyHooks;
import com.dataiku.dip.security.AuthCtx;
import com.dataiku.dip.connections.DSSConnection;
import com.dataiku.dip.coremodel.Dataset;
import com.dataiku.dip.coremodel.SerializedDataset;
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
import com.google.gson.JsonSyntaxException;
import org.json.JSONObject;


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

            SerializedDataset ds = (SerializedDataset)after;
            JsonElement dsConnectionElt = new JsonParser().parse(ds.getParams().getConnection());

            // If the list contains at least one column name check if dataset schema contains columns mapped in the ES index template
            if (connections.contains(dsConnectionElt.getAsJsonPrimitive()) && columnsList.length > 0) {
                
                JSONObject inferedDatasetEsMappingProperties = ElasticSearchUtils.getDefaultMappingDefinition(Dataset.fromSerialized(ds), ElasticSearchDialect.ES_7).getJSONObject("properties");
                
                String customMappingProperties = ds.getParamsAs(ElasticSearchDatasetHandler.Config.class).customMapping;
                JSONObject customMappingPropertiesObj = null;
                if(customMappingProperties != null && !customMappingProperties.trim().isEmpty() && isValidElasticsearchMapping(customMappingProperties)) {
                    customMappingPropertiesObj = new JSONObject(customMappingProperties);
                }

                // Use custom mapping if it exists to allow field type mapping customization for ES type not implemented in DSS 
                JSONObject sourceMappingProperties = (customMappingPropertiesObj != null ) ? customMappingPropertiesObj : inferedDatasetEsMappingProperties;


                // A custom ES mapping will only be required if the dataset contains columns mapped in an index template.
                boolean customEsMappingRequired = false;

                // For each column defined in the index template test if dataset schema contains it,
                // remove it from the dataset ES index mapping, and flag custom ES mapping as required.
                for (String col : columnsList) {
                    if(sourceMappingProperties.has(col)) {
                        sourceMappingProperties.remove(col);
                        customEsMappingRequired = true;
                    }
                }  
                 
                // Build the custom Elasticsearch JSON mapping if required and write it to the Elasticsearch connection parameters of the dataset
                if (customEsMappingRequired) {
                    JSONObject customDatasetEsMapping = new JSONObject();
                    customDatasetEsMapping.put("properties", sourceMappingProperties);
                    ds.getParamsAs(ElasticSearchDatasetHandler.Config.class).customMapping = customDatasetEsMapping.toString(0);
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
            return JsonParser.parseString(json).isJsonObject();
        } catch (JsonSyntaxException e) {
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
