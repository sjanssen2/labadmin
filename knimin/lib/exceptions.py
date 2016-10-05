from __future__ import division
import warnings


class LabadminError(Exception):
    """Base clase for all Labadmin exceptions"""
    pass


class LabadminDBError(LabadminError):
    """Base class for all Labadmin_db exceptions"""
    pass


# class LabadminDBNotImplementedError(LabadminDBError):
#     """"""
#     pass
#
#
# class LabadminDBExecutionError(LabadminDBError):
#     """Exception for error when executing SQL queries"""
#     pass
#
#
# class LabadminDBConnectionError(LabadminDBError):
#     """Exception for error when connecting to the db"""
#     pass
#
#
# class LabadminDBColumnError(LabadminDBError):
#     """Exception when missing table information or excess information passed"""
#     pass
#
#
# class LabadminDBLookupError(LabadminDBError, LookupError):
#     """Exception when converting or getting non-existant values in DB"""
#     pass
#
#
# class LabadminDBOperationNotPermittedError(LabadminDBError):
#     """Exception when perofrming an operation not permitted"""
#     pass
#
#
# class LabadminDBArtifactCreationError(LabadminDBError):
#     """Exception when creating an artifact"""
#     def __init__(self, reason):
#         super(LabadminDBArtifactCreationError, self).__init__()
#         self.args = ("Cannot create artifact: %s" % reason,)
#
#
# class LabadminDBArtifactDeletionError(LabadminDBError):
#     """Exception when deleting an artifact"""
#     def __init__(self, a_id, reason):
#         super(LabadminDBArtifactDeletionError, self).__init__()
#         self.args = ("Cannot delete artifact %d: %s" % (a_id, reason),)


class LabadminDBDuplicateError(LabadminDBError):
    """Exception when duplicating something in the database"""
    def __init__(self, obj_name, attributes):
        super(LabadminDBDuplicateError, self).__init__()
        self.args = ("The '%s' object with attributes (%s) already exists."
                     % (obj_name, attributes),)


# class LabadminDBStatusError(LabadminDBError):
#     """Exception when editing is done with an unallowed status"""
#     pass
#
#
class LabadminDBUnknownIDError(LabadminDBError):
    """Exception for error when an object does not exists in the DB"""
    def __init__(self, missing_id, table):
        super(LabadminDBUnknownIDError, self).__init__()
        self.args = ("The object with ID '%s' does not exists in table '%s'"
                     % (missing_id, table),)


# class LabadminDBDuplicateHeaderError(LabadminDBError):
#     """Exception for error when a MetadataTemplate has duplicate columns"""
#     def __init__(self, repeated_headers):
#         super(LabadminDBDuplicateHeaderError, self).__init__()
#         self.args = ("Duplicate headers found in MetadataTemplate. Note "
#                      "that the headers are not case-sensitive, repeated "
#                      "header(s): %s." % ', '.join(repeated_headers),)
#
#
# class LabadminDBDuplicateSamplesError(LabadminDBError):
#     """Exception for error when a MetadataTemplate has duplicate columns"""
#     def __init__(self, repeated_samples):
#         super(LabadminDBDuplicateSamplesError, self).__init__()
#         self.args = ("Duplicate samples found in MetadataTemplate: %s."
#                      % ', '.join(repeated_samples),)
#
#
# class LabadminDBIncompatibleDatatypeError(LabadminDBError):
#     """When arguments are used with incompatible operators in a query"""
#     def __init__(self, operator, argument_type):
#         super(LabadminDBIncompatibleDatatypeError, self).__init__()
#         self.args = ("The %s operator is not for use with data of type %s" %
#                      (operator, str(argument_type)))
#
#
class LabadminDBWarning(UserWarning):
    """Warning specific for the LabadminDB domain"""
    pass

warnings.simplefilter('always', LabadminDBWarning)
