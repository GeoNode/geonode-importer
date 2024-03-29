import logging

from geonode.resource.enumerator import ExecutionRequestAction as exa
from geonode.upload.api.exceptions import UploadParallelismLimitException
from geonode.upload.utils import UploadLimitValidator
from geopackage_validator.validate import validate
from importer.handlers.gpkg.exceptions import InvalidGeopackageException
from osgeo import ogr

from importer.handlers.common.vector import BaseVectorFileHandler
from importer.utils import ImporterRequestAction as ira

logger = logging.getLogger(__name__)


class GPKGFileHandler(BaseVectorFileHandler):
    """
    Handler to import GPK files into GeoNode data db
    It must provide the task_lists required to comple the upload
    """

    ACTIONS = {
        exa.IMPORT.value: (
            "start_import",
            "importer.import_resource",
            "importer.publish_resource",
            "importer.create_geonode_resource",
        ),
        exa.COPY.value: (
            "start_copy",
            "importer.copy_dynamic_model",
            "importer.copy_geonode_data_table",
            "importer.publish_resource",
            "importer.copy_geonode_resource",
        ),
        ira.ROLLBACK.value: (
            "start_rollback",
            "importer.rollback",
        ),
    }

    @property
    def supported_file_extension_config(self):
        return {
            "id": "gpkg",
            "label": "GeoPackage",
            "format": "vector",
            "ext": ["gpkg"],
        }

    @property
    def can_handle_xml_file(self) -> bool:
        """
        True or false if the handler is able to handle XML file
        By default a common workflow is always defined
        To be override if some expection are needed
        """
        return False

    @staticmethod
    def can_handle(_data) -> bool:
        """
        This endpoint will return True or False if with the info provided
        the handler is able to handle the file or not
        """
        base = _data.get("base_file")
        if not base:
            return False
        return (
            base.endswith(".gpkg")
            if isinstance(base, str)
            else base.name.endswith(".gpkg")
        )

    @staticmethod
    def is_valid(files, user):
        """
        Define basic validation steps:
        Upload limit:
            - raise exception if the layer number of the gpkg is greater than the max upload per user
            - raise exception if the actual upload + the gpgk layer is greater than the max upload limit

        Gpkg definition:
            Codes table definition is here: https://github.com/PDOK/geopackage-validator#what-does-it-do
            RQ1: Layer names must start with a letter, and valid characters are lowercase a-z, numbers or underscores.
            RQ2: Layers must have at least one feature.
            RQ13: It is required to give all GEOMETRY features the same default spatial reference system
            RQ14: The geometry_type_name from the gpkg_geometry_columns table must be one of POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, or MULTIPOLYGON
            RQ15: All table geometries must match the geometry_type_name from the gpkg_geometry_columns table
            RC18: It is recommended to give all GEOMETRY type columns the same name.
        """
        # calling base validation checks
        BaseVectorFileHandler.is_valid(files, user)
        # getting the upload limit validation
        upload_validator = UploadLimitValidator(user)
        upload_validator.validate_parallelism_limit_per_user()
        actual_upload = upload_validator._get_parallel_uploads_count()
        max_upload = upload_validator._get_max_parallel_uploads()

        layers = GPKGFileHandler().get_ogr2ogr_driver().Open(files.get("base_file"))

        if not layers:
            raise InvalidGeopackageException("The geopackage provided is invalid")

        layers_count = len(layers)

        if layers_count >= max_upload:
            raise UploadParallelismLimitException(
                detail=f"The number of layers in the gpkg {layers_count} is greater than "
                f"the max parallel upload permitted: {max_upload} "
                f"please upload a smaller file"
            )
        elif layers_count + actual_upload >= max_upload:
            raise UploadParallelismLimitException(
                detail=f"With the provided gpkg, the number of max parallel upload will exceed the limit of {max_upload}"
            )

        validator = validate(
            gpkg_path=files.get("base_file"),
            validations="RQ1, RQ2, RQ13, RQ14, RQ15, RC18",
        )
        if not validator[-1]:
            error_to_raise = []
            for error in validator[0]:
                logger.error(error)
                if "locations" in error:
                    error_to_raise.extend(error["locations"])
                else:
                    error_to_raise.append(error["validation_description"])

            raise InvalidGeopackageException(". ".join(error_to_raise))

        return True

    def get_ogr2ogr_driver(self):
        return ogr.GetDriverByName("GPKG")

    def handle_xml_file(self, saved_dataset, _exec):
        """
        Not implemented for GPKG, skipping
        """
        pass
