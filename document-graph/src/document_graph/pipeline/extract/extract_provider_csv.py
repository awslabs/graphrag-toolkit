# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Extract Provider Csv module for document graph operations."""

import logging
from pathlib import Path
from typing import List, Tuple


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available
# logging via graphrag-toolkit  # noqa: F401
from document_graph.config import DocumentGraphConfig
from document_graph.pipeline.extract.extract_provider_base import ExtractProvider
from document_graph.pipeline.extract.extract_provider_config import ExtractProviderConfig
from document_graph.schema.providers.csv_schema_provider import CSVSchemaProvider
from document_graph.schema.etl_schema_model import ETLSchema
from document_graph.pipeline.extract.utils.dataframe_reader import DataFrameReader
# PathResolver removed



from document_graph.schema.providers.schema_provider_config import SchemaProviderConfig



class CSVExtractProvider(ExtractProvider):
    """
    Provide functionality to extract data from a CSV file based on a specified ETL configuration.

    This class is responsible for handling CSV files, supporting local file paths or S3 URIs,
    and extracting structured information while adhering to ETL schema configurations. It can
    apply data transformations, infer schema, save skipped rows if necessary, and stream process
    data when ingestors are provided.

    Attributes:
        config (ExtractProviderConfig): Configuration details for the extraction provider.
        aws_config (DocumentGraphConfig): AWS settings for operations like S3 file handling.
    """

    def __init__(self, config: ExtractProviderConfig, aws_config: DocumentGraphConfig):
        """
        Initializes an instance of CSVExtractProvider.

        The initializer sets up the CSVExtractProvider with the provided configuration
        objects. It logs the configurations and calls the parent class initializer
        to ensure proper initialization.

        Parameters:
            config (ExtractProviderConfig): Configuration specific to the extraction provider.
            aws_config (DocumentGraphConfig): Configuration related to AWS Document Graph services.
        """
        logger.debug(f"Initializing CSVExtractProvider with config: {vars(config)}")
        logger.debug(f"AWS config: {vars(aws_config)}")
        super().__init__(config, aws_config)
        logger.debug(f"[CSVExtractProvider] args: {self.config.args}")
        logger.debug(f"[CSVExtractProvider] pipeline_args: {self.config.pipeline_args}")

    def extract(self, source: str, ingestors=None, **kwargs) -> None:
        """
        Extracts data from a CSV file at the given source and processes it into a structured format.
        The extraction process includes validating the file's existence, handling any parsing errors,
        and applying configurations for data transformation. Optionally, it integrates with ETL schemas,
        infers the schema from the data, and generates document nodes for downstream processes.

        The function achieves robust CSV parsing and ensures compatibility with specific configurations,
        including handling row limits and bad line strategies.

        Parameters:
            source (str): The source path of the CSV file to extract data from.
            ingestors: Optional list of ingestors to process data in a streaming fashion.
            kwargs: Arbitrary keyword arguments for additional extraction configurations.

        Raises:
            Exception: If errors occur during file parsing or data inference.

        Returns:
            None
        """
        logger.debug(f"[extract] Entry point kwargs: {kwargs}")
        logger.debug(f"[extract] config.args received: {self.config.args}")
        logger.debug(f"[extract] config.pipeline_args received: {self.config.pipeline_args}")

        logger.info(f"Starting CSV extraction from source: {source}")
        logger.debug(f"extract called with kwargs: {kwargs}")
        resolved_path = source
        resolved_path = Path(resolved_path)
        logger.debug(f"Resolved path: {resolved_path}")

        # Use centralized error handling
        # ErrorHandler removed
        assert Path(resolved_path).exists(), f"File not found: {resolved_path}"

        try:
            # Filter out custom parameters before passing to pandas
            logger.debug(f"Original config.args: {self.config.args}")
            pandas_args = {
                k: v for k, v in self.config.args.items() if k != 'output_skip_lines'
            }
            
            # Add header config from transformers but never row limits
            logger.debug(f"Transformers config: {self.config.transformers}")
            if self.config.transformers:
                for transformer in self.config.transformers:
                    if transformer.get('type') == 'row_to_node':
                        transformer_config = transformer.get('config', {})
                        # Keep header override if explicitly provided
                        if 'header' in transformer_config:
                            pandas_args['header'] = transformer_config['header']
            
            # Never let Transformers set row limits for Extract
            pandas_args.pop('nrows', None)
            pandas_args.pop('skiprows', None)
            
            logger.info(f"Final pandas_args: {pandas_args}")

            # Ensure pandas uses the correct engine for robust CSV parsing
            if pandas_args.get("on_bad_lines") == "skip":
                current_engine = pandas_args.get("engine", "c").lower()
                if current_engine != "python":
                    logger.warning(
                        "on_bad_lines='skip' requires engine='python'; overriding engine to ensure compatibility."
                    )
                    pandas_args["engine"] = "python"
                    logger.debug(f"Overridden pandas_args with engine='python': {pandas_args}")

            # Use streaming if ingestors provided
            logger.debug(f"Ingestors parameter: {ingestors}")
            logger.debug(f"Ingestors type: {type(ingestors)}")
            if ingestors:
                logger.info("Using streaming extraction with ingestors")
                df = self._stream_with_ingestors(resolved_path, pandas_args, ingestors)
            else:
                logger.info("No ingestors provided, using regular extraction")
                logger.debug(f"Reading CSV with final pandas args: {pandas_args}")
                df = DataFrameReader(path=resolved_path, **pandas_args).read()
            
            logger.info(f"Successfully read CSV with shape: {df.shape}")
            logger.debug(f"DataFrame dtypes: {df.dtypes.to_dict()}")
            logger.debug(f"DataFrame head(2): {df.head(2).to_dict(orient='records')}")

            # Save skipped lines if requested
            if self.config.args.get('output_skip_lines', False) and self.config.args.get('on_bad_lines') == 'skip':
                self._save_skipped_lines(resolved_path)

        except Exception as e:
            logger.error(f"Error during CSV parsing: {e}", exc_info=True)
            raise RuntimeError(f"CSV parse error: {resolved_path}: {e}") from e

        if df.empty:
            strict_mode = kwargs.get("strict", True)
            logger.warning(f"DataFrame is empty, strict_mode: {strict_mode}")
            error = ValueError(f"Empty data: {resolved_path}") if strict_mode else None
            if error:
                raise error
            return [], {}

        logger.debug("Inferring schema from DataFrame")
        schema = {col: str(df[col].dtype) for col in df.columns}
        
        logger.debug(f"Inferred schema with {len(schema)} fields: {schema}")

        logger.debug(f"Converting {len(df)} rows to document nodes")
        nodes = []
        for i, row in df.iterrows():
            content = ", ".join(f"{k}: {v}" for k, v in row.items())
            node = dict(
                id=f"{self.config.document_id}-row-{i}",
                type="Row",
                content=content,
                metadata={"row_index": str(i)})
            nodes.append(node)
        if nodes:
            logger.debug(f"Sample node: id={nodes[0]['id']}, type={nodes[0]['type']}, metadata={nodes[0]['metadata']}")

        # Load ETL schema if configured
        etl_schema = self.config.load_schema_if_needed()
        if not etl_schema:
            # Dynamically generate schema_id from document_id
            document_id = self.config.document_id
            schema_id = f"{document_id}-schema" if document_id else None
            logger.debug(f"Generated schema_id: {schema_id}")

            # Separate args for schema discovery (exclude batching parameters)
            schema_args = {k: v for k, v in pandas_args.items() 
                          if k not in ['skiprows', 'nrows']}
            
            # Create schema from CSV file using SchemaProviderConfig with args
            schema_config = SchemaProviderConfig(
                type="csv",
                schema_id=schema_id,
                connection_config={"path": str(resolved_path), "args": schema_args}
            )
            logger.debug(f"SchemaProviderConfig: {vars(schema_config)}")
            csv_schema_provider = CSVSchemaProvider(schema_config)
            etl_schema = csv_schema_provider.load_schema()
            logger.info(f"Generated ETL schema from CSV: {etl_schema.schema_id}")
            logger.debug(f"ETL schema details: {vars(etl_schema)}")

        # Create extraction result with ETL schema integration
        from document_graph.pipeline.extract.extraction_result import ExtractionResult

        metadata = {
            "source": source,
            "extraction_type": "csv",
            "etl_schema_id": etl_schema.schema_id if etl_schema else None,
            "rows_processed": str(len(df)),
            "schema_fields": str(len(schema))
        }
        logger.debug(f"Extraction metadata: {metadata}")

        result = ExtractionResult(
            document_id=self.config.document_id,
            extracted_schema=schema,
            nodes=nodes,
            dataframe=df,
            metadata=metadata
        )

        logger.info(
            f"CSV extraction completed: {len(nodes)} nodes, ETL schema: {etl_schema.schema_id if etl_schema else 'None'}")
        return result

    def _save_skipped_lines(self, csv_path: Path):
        """
        Saves lines from a CSV file that were skipped during parsing to a separate file.

        This method processes a given CSV file, identifies lines that do not match the
        expected field count based on the file's header, and writes the invalid lines to a
        new file. The new file contains information about the skipped lines and their
        discrepancies.

        Attributes
        ----------
        None

        Parameters
        ----------
        csv_path : Path
            Path to the CSV file to be checked for skipped lines.

        Raises
        ------
        Exception
            If an exception occurs during reading, processing, or writing skipped lines,
            it logs the warning including an exception traceback.

        Note
        ----
        The output file containing the skipped lines is named with a `.skip` suffix before
        the original file's extension.
        """
        import pandas as pd
        import csv

        skip_file = csv_path.with_suffix('.skip' + csv_path.suffix)
        logger.debug(f"Checking for skipped lines, output: {skip_file}")

        try:
            # Read all lines
            with open(csv_path, 'r', encoding='utf-8') as f:
                all_lines = f.readlines()
            logger.debug(f"Total lines in file: {len(all_lines)}")

            # Read successfully parsed lines
            pandas_args = {k: v for k, v in self.config.args.items() if k != 'output_skip_lines'}
            logger.debug(f"pandas_args for reading good_df: {pandas_args}")
            good_df = pd.read_csv(csv_path, **pandas_args)
            logger.debug(f"Good DataFrame shape: {good_df.shape}")

            # Detect delimiter from first line
            sniffer = csv.Sniffer()
            delimiter = sniffer.sniff(all_lines[0]).delimiter if all_lines else ','
            logger.debug(f"Detected delimiter: '{delimiter}'")

            # Find lines with wrong field count
            header_fields = len(all_lines[0].split(delimiter)) if all_lines else 0
            logger.debug(f"Header fields count: {header_fields}")
            skipped_lines = []

            for i, line in enumerate(all_lines[1:], 1):  # Skip header
                field_count = len(line.split(delimiter))
                if field_count != header_fields:
                    skipped_lines.append(f"# Line {i + 1}: Expected {header_fields} fields, got {field_count}\n")
                    skipped_lines.append(line)

            # Save skipped lines if any found
            if skipped_lines:
                with open(skip_file, 'w', encoding='utf-8') as f:
                    f.write(f"# Skipped lines from {csv_path.name}\n")
                    f.write(f"# Header: {all_lines[0]}")
                    f.writelines(skipped_lines)
                logger.info(f"Saved {len(skipped_lines) // 2} skipped lines to: {skip_file}")
            else:
                logger.debug("No skipped lines detected")

        except Exception as e:
            logger.warning(f"Could not save skipped lines: {e}", exc_info=True)
    
    def _stream_with_ingestors(self, csv_path, pandas_args, ingestors):
        """
        Handles reading and processing a CSV file with error tolerance and memory handling.

        The method attempts to read the entire file first, applying custom error handling on bad lines.
        If the full read fails, it falls back to reading the file in chunks, leveraging chunked processing
        to handle memory constraints. The method integrates the use of ingestors for data filtering
        and transformation, which is applied to data either fully or in chunks.

        Attributes:
            csv_path (str): The path to the CSV file to be read.
            pandas_args (dict): Additional arguments passed to pandas read_csv for configuration.
            ingestors: An object or utility providing the `execute` method for applying transformations or
                filtering to the loaded data.

        Parameters and Configuration:
            csv_path: The file path of the CSV to process.
            pandas_args: Contains any additional arguments needed for `pd.read_csv`. The function utilizes
            these for fallback operations if required.
            ingestors: Utilized to process and filter the read DataFrame.

        Raises:
            Various exceptions may be generated on initial full read-pandas arguments under directly/during
        ```"""
        import pandas as pd
        
        # First try: Load entire file with robust error handling
        logger.info(f"Attempting full file read: {csv_path}")
        try:
            # Custom bad line handler for debugging
            def bad_line_handler(line):
                """
                Handles lines considered as "bad" by logging a warning.

                This function is used to process lines that are identified as problematic
                or invalid for further operations. It logs a warning with detailed
                information about the bad line and then skips it by returning None.

                Parameters:
                line (str): The input line identified as bad. The first 200 characters
                of this line will be included in the warning message for debugging purposes.

                Returns:
                None: Indicates that the bad line has been skipped.
                """
                logger.warning(f"Skipping bad line: {repr(line[:200])}...")
                return None
            
            # Read entire file with maximum error tolerance
            full_df = pd.read_csv(
                csv_path, 
                encoding='utf-8',
                engine='python',
                on_bad_lines=bad_line_handler,
                low_memory=False,
                quoting=1,  # QUOTE_ALL
                skipinitialspace=True
            )
            logger.info(f"Full file loaded: {len(full_df)} rows")
            
            # Apply ingestors to full dataset
            filtered_df = ingestors.execute(full_df)
            logger.info(f"Ingestors applied: {len(full_df)} -> {len(filtered_df)} rows")
            return filtered_df
            
        except Exception as e:
            logger.warning(f"Full file read failed: {e}, trying chunked approach")
            
        # Fallback: Try chunked reading
        chunk_size = 10000
        filtered_chunks = []
        chunk_args = {'encoding': pandas_args.get('encoding', 'utf-8')}
        
        logger.info(f"Pandas chunked reading: {csv_path}")
        logger.info(f"Chunk args: {chunk_args}")
        
        try:
            chunk_reader = pd.read_csv(csv_path, chunksize=chunk_size, iterator=True, **chunk_args)
            
            chunk_count = 0
            total_rows_read = 0
            
            while True:
                try:
                    chunk = next(chunk_reader)
                    chunk_count += 1
                    total_rows_read += len(chunk)
                    logger.info(f"Processing chunk {chunk_count}: {len(chunk)} rows (total: {total_rows_read})")
                    
                    filtered_chunk = ingestors.execute(chunk)
                    if not filtered_chunk.empty:
                        filtered_chunks.append(filtered_chunk)
                        
                except StopIteration:
                    logger.info("Reached end of file - no more chunks")
                    break
            
            logger.info(f"Pandas chunked reading completed: {chunk_count} chunks, {total_rows_read} total rows")
            
            if filtered_chunks:
                result = pd.concat(filtered_chunks, ignore_index=True)
                logger.info(f"Combined {len(filtered_chunks)} chunks -> {len(result)} rows")
                return result
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Pandas chunked reading failed: {e}", exc_info=True)
            logger.warning("Falling back to pure pandas read_csv")
            return pd.read_csv(csv_path, **pandas_args)
    

    
