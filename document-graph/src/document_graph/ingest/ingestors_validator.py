"""Ingestors validator — validates ingestor configurations before execution."""
import logging
from typing import List, Dict, Any
from document_graph.ingest.ingestors_provider_config import IngestorProviderConfig
from document_graph.ingest.ingestors_provider_registry import IngestorProviderRegistry

# Use document-graph logging
logger = logging.getLogger(__name__)

class IngestorConfigValidator:
    """
    This class provides methods to validate ingestor configurations for an ingestion pipeline.

    The class aims to ensure that the configuration settings for different types of ingestors are
    compatible and meet the required standards. It validates individual configuration parameters,
    checks for missing required fields, and ensures there are no dependency conflicts between
    multiple ingestors. This helps ensure a robust and error-free ingestion process.
    """
    
    # Required fields for each ingestor type
    REQUIRED_FIELDS = {
        "skip_row": ["conditions"],
        "date_range_filter": ["date_field"],
        "column_selector": ["columns"],
        "column_renamer": ["mapping"],
        "column_reorder": ["column_order"],
        "column_type_converter": ["type_mapping"]
    }
    
    @classmethod
    def validate_config(cls, config: IngestorProviderConfig) -> List[str]:
        """
        Validates the configuration for an ingestor type.

        This method checks if the provided ingestor type is registered, ensures that
        all required fields for the specified ingestor type are present, and performs
        type-specific validations based on the type of the ingestor. Errors found
        during the validation process are collected and returned as a list of strings.

        Args:
            config (IngestorProviderConfig): Configuration object containing the type
            of the ingestor and its arguments.

        Returns:
            List[str]: A list of error messages, if any validation issues are found.
        """
        errors = []
        
        # Check if ingestor type is registered
        try:
            IngestorProviderRegistry.get(config.type)
        except ValueError:
            errors.append(f"Unknown ingestor type: {config.type}")
            return errors
        
        # Check required fields
        required = cls.REQUIRED_FIELDS.get(config.type, [])
        for field in required:
            if field not in config.args:
                errors.append(f"{config.type}: Missing required field '{field}'")
        
        # Type-specific validation
        if config.type == "skip_row":
            errors.extend(cls._validate_skip_row(config.args))
        elif config.type == "date_range_filter":
            errors.extend(cls._validate_date_range_filter(config.args))
        elif config.type == "column_selector":
            errors.extend(cls._validate_column_selector(config.args))
        
        return errors
    
    @classmethod
    def validate_configs(cls, configs: List[IngestorProviderConfig]) -> List[str]:
        """
        Validates a list of ingestor configuration objects and checks for possible issues.

        This method performs validation on each provided configuration by using
        the `validate_config` method. It also checks for dependency conflicts
        among configurations and collects any validation errors.

        Args:
            configs (List[IngestorProviderConfig]): A list of configuration objects
                to be validated.

        Returns:
            List[str]: A list of error messages for invalid configurations, or an
                empty list if all configurations are valid.
        """
        all_errors = []
        
        for i, config in enumerate(configs):
            errors = cls.validate_config(config)
            for error in errors:
                all_errors.append(f"Ingestor {i+1} ({config.name}): {error}")
        
        # Check for dependency conflicts
        all_errors.extend(cls._check_dependencies(configs))
        
        return all_errors
    
    @classmethod
    def _validate_skip_row(cls, args: Dict[str, Any]) -> List[str]:
        """
        Validates the provided conditions for skipping rows against the required
        format and applicable operator requirements.

        The method checks if the 'conditions' in `args` is a list and validates each
        condition entry to ensure it is a dictionary containing required keys such as
        'field' and 'operator'. It also ensures that specific operators have associated
        'required' values. Returns a list of error messages detailing any validation
        failures.

        Arguments:
            args (Dict[str, Any]): A dictionary containing the conditions to validate.

        Returns:
            List[str]: A list of error messages resulting from the validation process.
        """
        errors = []
        conditions = args.get("conditions", [])
        
        if not isinstance(conditions, list):
            errors.append("conditions must be a list")
            return errors
        
        for i, condition in enumerate(conditions):
            if not isinstance(condition, dict):
                errors.append(f"Condition {i+1} must be a dictionary")
                continue
            
            if "field" not in condition:
                errors.append(f"Condition {i+1}: Missing 'field'")
            if "operator" not in condition:
                errors.append(f"Condition {i+1}: Missing 'operator'")
            
            operator = condition.get("operator")
            if operator in ["eq", "ne", "lt", "le", "gt", "ge", "in", "not_in", "contains", "not_contains", "startswith", "endswith", "regex", "length_eq", "length_gt", "length_lt"]:
                if "value" not in condition:
                    errors.append(f"Condition {i+1}: Operator '{operator}' requires 'value'")
        
        return errors
    
    @classmethod
    def _validate_date_range_filter(cls, args: Dict[str, Any]) -> List[str]:
        """
        Validates the presence of at least one date constraint in the given arguments.

        This method ensures that the arguments dictionary contains at least one of the
        specified date constraint keys: "start_date", "end_date", "days_back",
        "weeks_back", or "months_back". If none of these keys are present, it
        accumulates an error message.

        Args:
            args (Dict[str, Any]): The dictionary of arguments to be validated.

        Returns:
            List[str]: A list of error messages. An empty list indicates that the
            validation was successful.
        """
        errors = []
        
        # Must have at least one date constraint
        date_constraints = ["start_date", "end_date", "days_back", "weeks_back", "months_back"]
        if not any(constraint in args for constraint in date_constraints):
            errors.append("Must specify at least one date constraint")
        
        return errors
    
    @classmethod
    def _validate_column_selector(cls, args: Dict[str, Any]) -> List[str]:
        """
        Validates the column selector arguments and returns a list of any detected
        validation errors. This method ensures that the provided arguments for the
        column selection or dropping process conform to the expected format and
        requirements.

        Parameters:
            args (Dict[str, Any]): A dictionary containing the arguments for the
            column selector. Must include 'action' and 'columns'.

        Returns:
            List[str]: A list of errors as strings, if any. An empty list is returned
            if no errors are detected.
        """
        errors = []
        
        action = args.get("action", "select")
        if action not in ["select", "drop"]:
            errors.append(f"Invalid action '{action}'. Must be 'select' or 'drop'")
        
        columns = args.get("columns", [])
        if not isinstance(columns, list):
            errors.append("columns must be a list")
        elif not columns:
            errors.append("columns list cannot be empty")
        
        return errors
    
    @classmethod
    def _check_dependencies(cls, configs: List[IngestorProviderConfig]) -> List[str]:
        """
        Checks for dependencies and conflicts among a list of ingestor configurations.

        This method ensures that column operations, such as renaming and filtering, do not
        depend on columns that have been removed by prior operations. It analyzes the sequence
        of configurations and identifies any conflicting dependencies which might cause runtime
        issues.

        Parameters:
        configs : List[IngestorProviderConfig]
            List of configurations for ingestors that define various operations such as
            column selection, renaming, filtering, etc.

        Returns:
        List[str]
            A list of error messages describing any conflicts detected between column operations
            performed by the ingestors. Each message specifies the index and type of conflict
            in the configurations.
        """
        errors = []
        
        # Check for column operations after column removal
        column_removers = []
        column_users = []
        
        for i, config in enumerate(configs):
            if config.type == "column_selector" and config.args.get("action") == "drop":
                column_removers.append((i, config.args.get("columns", [])))
            elif config.type in ["column_renamer", "skip_row", "date_range_filter"]:
                if config.type == "column_renamer":
                    column_users.append((i, list(config.args.get("mapping", {}).keys())))
                elif config.type == "skip_row":
                    fields = [cond.get("field") for cond in config.args.get("conditions", [])]
                    column_users.append((i, fields))
                elif config.type == "date_range_filter":
                    column_users.append((i, [config.args.get("date_field")]))
        
        # Check if any column user comes after a column remover that removes its columns
        for user_idx, user_columns in column_users:
            for remover_idx, removed_columns in column_removers:
                if remover_idx < user_idx:  # Remover comes before user
                    conflicts = set(user_columns) & set(removed_columns)
                    if conflicts:
                        errors.append(f"Ingestor {user_idx+1} uses columns {list(conflicts)} that were removed by ingestor {remover_idx+1}")
        
        return errors