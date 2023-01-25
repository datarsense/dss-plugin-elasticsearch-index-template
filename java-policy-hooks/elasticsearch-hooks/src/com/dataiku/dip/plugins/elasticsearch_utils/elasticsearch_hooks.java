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
import org.json.JSONObject;


public class elasticsearch_hooks extends CustomPolicyHooks {
    
    private static DKULogger logger = DKULogger.getLogger("dku.plugins.elasticsearch.hooks");
    
    @Autowired private RegularPluginsRegistryService regularPluginsRegistryService;

    @Override
    public void onPreObjectSave(AuthCtx user, TaggableObject before, TaggableObject after) throws Exception {
        if(after.getClass() == SerializedDataset.class && this.isElasticSearchDataset((SerializedDataset)after)) {
            // Get a list of columns defined in an ES index template
            JsonObject pluginSettings = regularPluginsRegistryService.getSettings("elasticsearch-index-template").config;
            boolean hasColumnsKey = pluginSettings != null && pluginSettings.has("columns");
            String columns = hasColumnsKey ? pluginSettings.get("columns").getAsString() : null;
            String[] columnsList = columns.split(",");

            // If the list contains at least one column name check if dataset schema contains columns mapped in the ES index template
            if (columnsList.length > 0) {
                SerializedDataset ds = (SerializedDataset)after;
                JSONObject inferedDatasetEsMappingProperties = ElasticSearchUtils.getDefaultMappingDefinition(Dataset.fromSerialized(ds), ElasticSearchDialect.ES_7).getJSONObject("properties");
                logger.info((Object)("DATASET INFERED MAPPING PROPERTIES " + inferedDatasetEsMappingProperties));
                
                // A custom ES mapping will only be required if the dataset contains columns mapped in an index template.
                boolean customEsMappingRequired = false;

                // For each column defined in the index template test if dataset schema contains it,
                // remove it from the dataset ES index mapping, and flag custom ES mapping as required.
                for (String col : columnsList) {
                    if(inferedDatasetEsMappingProperties.has(col)) {
                        inferedDatasetEsMappingProperties.remove(col);
                        customEsMappingRequired = true;
                    }
                }  
                 
                // Build the custom Elasticsearch JSON mapping if required and write it to the Elasticsearch connection parameters of the dataset
                if (customEsMappingRequired) {
                    JSONObject customDatasetEsMapping = new JSONObject();
                    customDatasetEsMapping.put("properties", inferedDatasetEsMappingProperties);
                    ds.getParamsAs(ElasticSearchDatasetHandler.Config.class).customMapping = customDatasetEsMapping.toString(0);
                    logger.info((Object)("WRITE CUSTOM MAPPING " + ((ElasticSearchDatasetHandler.Config)ds.getParams()).customMapping));
                }
            }
        }
    }
    
    private boolean isElasticSearchDataset(SerializedDataset ds) {
        return ds.type.equals("ElasticSearch");
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
