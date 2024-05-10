from typing import Dict, List


class NoTasksLoadedError(Exception):
    pass


class ExtensionsDirectoryNotFoundError(Exception):
    pass


class NotAPackageError(Exception):
    pass


class CheckpointInvalidError(Exception):
    pass


class TaskValidationError(Exception):
    def __init__(self, *args, **kwargs):
        self._errors: List[Dict[str, str]] = []
        super().__init__(*args, **kwargs)

    def __str__(self) -> str:
        msg = ' '.join(self.args)
        for e in self._errors:
            msg += f'\n - {e["field"]}: {e["msg"]}'
        return msg

    def add_error(self, field: str, msg: str) -> None:
        self._errors.append({'field': field, 'msg': msg})

    @property
    def errors(self) -> List[Dict[str, str]]:
        return self._errors


class VarFormatError(Exception):
    pass


class TaskClassNotFoundError(Exception):
    pass


class ModuleLoadError(Exception):
    pass
