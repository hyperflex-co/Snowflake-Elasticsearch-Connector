from elasticsearch import Elasticsearch, helpers
import logging


class ElasticClient:
    """
    A simple wrapper around the Elasticsearch Python client.
    This class handles:
      - connecting to Elasticsearch
      - checking connectivity (ping)
      - creating an index if needed
      - bulk indexing documents
      - bulk deleting documents
      - refreshing the index
    """

    def __init__(self, host, api_key=None, username=None, password=None, timeout=30, logger=None):
        print("Elasticsearch host param:", repr(host))

        self.logger = logger or logging.getLogger("ElasticClient")

        # Build the client for Elasticsearch 9.x
        if api_key:
            self.es = Elasticsearch(
                hosts=[host],
                api_key=api_key,
                verify_certs=False,
                ssl_show_warn=False,
                ssl_assert_hostname=False,
                ssl_assert_fingerprint=None,
                ca_certs=None,
            )
        else:
            self.es = Elasticsearch(
                hosts=[host],
                basic_auth=(username, password),
                verify_certs=False,
                ssl_show_warn=False,
                ssl_assert_hostname=False,
                ssl_assert_fingerprint=None,
                ca_certs=None,
            )


    def ping(self):
        """
        Check if Elasticsearch is reachable.
        Returns True if ES responds, False otherwise.
        """
        try:
            if self.es.ping():
                self.logger.info("Connected to Elasticsearch successfully")
                return True
            else:
                self.logger.error("Elasticsearch ping failed")
                return False
        except Exception as e:
            self.logger.exception(f"Error pinging Elasticsearch: {e}")
            return False

    def ensure_index(self, index_name, mappings=None, settings=None):
        """
        Create the index if it does not already exist.

        mappings: optional field mappings
        settings: optional index settings
        """
        try:
            # Only create the index if it doesn't exist
            if not self.es.indices.exists(index=index_name):
                body = {}

                if settings:
                    body["settings"] = settings
                if mappings:
                    body["mappings"] = mappings

                self.es.indices.create(index=index_name, body=body)
                self.logger.info(f"Created index: {index_name}")

        except Exception as e:
            self.logger.exception(f"Error ensuring index {index_name}: {e}")
            raise

    def bulk_index(self, index_name, docs):
        """
        Bulk index documents into Elasticsearch.

        docs should be a list of:
        {
            "_id": "document-id",
            "_source": { ... actual document data ... }
        }

        Returns the number of successfully indexed documents.
        """
        if not docs:
            return 0

        # Convert docs into Elasticsearch bulk API format
        actions = [
            {
                "_op_type": "index",   # "index" means insert or update
                "_index": index_name,
                "_id": doc["_id"],
                "_source": doc["_source"],
            }
            for doc in docs
        ]

        try:
            # helpers.bulk sends all actions in one efficient request
            success, errors = helpers.bulk(self.es, actions, raise_on_error=False)

            if errors:
                self.logger.error(f"Bulk indexing errors: {errors}")

            return success

        except Exception as e:
            self.logger.exception(f"Bulk indexing failed: {e}")
            raise

    def bulk_delete(self, index_name, ids):
        """
        Bulk delete documents by ID.

        ids should be a list of document IDs.
        """
        if not ids:
            return 0

        actions = [
            {
                "_op_type": "delete",
                "_index": index_name,
                "_id": doc_id,
            }
            for doc_id in ids
        ]

        try:
            success, errors = helpers.bulk(self.es, actions, raise_on_error=False)

            if errors:
                self.logger.error(f"Bulk delete errors: {errors}")

            return success

        except Exception as e:
            self.logger.exception(f"Bulk delete failed: {e}")
            raise

    def refresh(self, index_name):
        """
        Force Elasticsearch to refresh the index.
        This makes newly indexed documents searchable immediately.
        """
        try:
            self.es.indices.refresh(index=index_name)
        except Exception as e:
            self.logger.exception(f"Error refreshing index {index_name}: {e}")
