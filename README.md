# DSS Plugin - Elasticsearch index template

An **index template** is a way to tell Elasticsearch how to configure new indices with **predefined mappings and settings** when they are created.  Templates are configured prior to index creation. When an index is created - either manually or through indexing a document - the template settings are used as a basis for creating the index.

The purpose of this plugin is to customize the Elasticsearch mapping of an Elasticsearch DSS managed dataset to **avoid mapping conflict** between the Elasticsearch mapping infered by DSS for the dataset and the mapping configured by **index templates** in the target Elasticsearch cluster.


## Compatibility

DSS 11.0 or higher and Elasticsearch 7.x or higher are required.

## Usage
### Plugin configuration
The following parameter can be configured globally. It is applied to all the Elasticsearch managed datasets created on one of the connections defined in plugin presets:
* **Allow mapping customization** : Make DSS users able to define a custom Elasticsearch mapping for a managed DSS dataset. Useful to allow field type mapping customization for an Elasticsearch type not implemented in DSS such as **ip**.

**WARNING :** Custom mapping will be overwritten at each schema change. Using an index template is the recommended way to configure field type mapping to Elasticsearch types not implemented in DSS.


### Plugin presets
Multiple presets can be configured in plugin settings to support multiple connections featuring different index templates. The following settings have to be configured in each configuration preset :
* **Elasticsearch connections** : The name of the Elasticsearch connection(s) containing an Elasticsearch index template.. Multiple connections can be configured in a single preset.
* **Columns names** : Comma-separated list of column names defined in an Elasticsearch index template conflicting with DSS column mapping. 

An Elasticsearch connection can only be defined in a single preset. An exception is triggered when a new dataset is created if the dataset connection is defined in more than one configuration preset

