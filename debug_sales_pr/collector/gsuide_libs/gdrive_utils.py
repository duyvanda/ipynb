import os
import tqdm
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import pathlib

from collector.core.abstract_utils import AbstractUtils
from collector.libs.logger import get_logger

logger = get_logger(__name__)

MAP_GSUITE_MIMETYPES = {
    # Drive Document files as MS dox
    "application/vnd.google-apps.document": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    # Drive Sheets files as MS Excel files.
    "application/vnd.google-apps.spreadsheet": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    # Drive presentation as MS pptx
    "application/vnd.google-apps.presentation": "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    # see https://developers.google.com/drive/v3/web/mime-types
}

FOLDER_MINE = "application/vnd.google-apps.folder"
ZIP_MINE = "application/x-zip-compressed"
XLSX_MINE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
SHORTCUT_MINE = "application/vnd.google-apps.shortcut"

EXTENSTIONS = {
    "application/vnd.google-apps.document": ".docx",
    "application/vnd.google-apps.spreadsheet": ".xlsx",
    "application/vnd.google-apps.presentation": ".pptx",
}


def escape_fname(name):
    return name.replace("/", "-")


class GDriveUtils(AbstractUtils):
    def __init__(self, config: dict):
        super().__init__(config)
        setting_yml = config.get("setting_yml")
        self.gauth = GoogleAuth(settings_file=setting_yml)
        if config.get("refresh_token"):
            self.refresh_token()
        self.drive = GoogleDrive(self.gauth)
        # self.drive_out_dir = config['drive_out_dir']

    def refresh_token(self):
        self.gauth.LocalWebserverAuth()

    @staticmethod
    def mkdir_paths(ancestry, map_folder_name, drive_out_dir):
        map_folder_id = dict(map(lambda x: (x[1], x[0]), map_folder_name.items()))
        parents = set()
        children = dict()
        for c, p in ancestry:
            parents.add(p)
            children[c] = p

        # recursively determine parents until child has no parent
        def ancestors(parent):
            return (ancestors(children[parent]) if parent in children else []) + [
                parent
            ]

        for k in set(children.keys()) - parents:
            lst_path = ancestors(k)
            if len(lst_path) > 0:
                path = "/".join(lst_path)
                full_path = os.path.join(drive_out_dir, path)
                pathlib.Path(full_path).mkdir(parents=True, exist_ok=True)

        dct_folder_abs_path = dict()
        for k in set(children.keys()):
            lst_path = ancestors(k)
            path = "/".join(lst_path)
            full_path = os.path.join(drive_out_dir, path)
            dct_folder_abs_path[map_folder_id.get(k)] = full_path
        return dct_folder_abs_path

    def refresh_folder(self, dct_params, drive_out_dir):
        lst_files = self.drive.ListFile(dct_params).GetList()
        total = len(lst_files)
        lst_child_parent_id = list()
        map_folder_name = dict()
        with tqdm.tqdm(total=total) as pbar:
            for file in self.drive.ListFile(dct_params).GetList():
                if file["mimeType"] == FOLDER_MINE:
                    folder_id = file["id"]
                    map_folder_name[folder_id] = file["title"]
                    if file["parents"]:
                        for parent in file["parents"]:
                            parent_id = parent["id"]
                            if parent["isRoot"]:
                                # print(file['title'])
                                continue
                            lst_child_parent_id.append((folder_id, parent_id))
                pbar.update(1)

        lst_child_parent_name = list(
            map(
                lambda x: (map_folder_name.get(x[0]), map_folder_name.get(x[1])),
                lst_child_parent_id,
            )
        )
        dct_folder_abs_path = self.mkdir_paths(
            lst_child_parent_name, map_folder_name, drive_out_dir
        )
        return dct_folder_abs_path

    def down_load_files_match(self, dct_params, dct_folder_abs_path, drive_out_dir):
        lst_files = self.drive.ListFile(dct_params).GetList()
        total = len(lst_files)
        with tqdm.tqdm(total=total) as pbar:
            for file in lst_files:
                logger.info("download {0}".format(file['title']))
                out_dir = drive_out_dir
                if file["parents"]:
                    parent = file["parents"][0]
                    if not parent["isRoot"]:
                        out_dir = dct_folder_abs_path.get(parent["id"], out_dir)
                    out_path = os.path.join(out_dir, escape_fname(file["title"]))
                else:
                    out_path = os.path.join(out_dir, escape_fname(file["title"]))

                if file["mimeType"] in MAP_GSUITE_MIMETYPES:
                    mimetype = MAP_GSUITE_MIMETYPES[file["mimeType"]]
                    out_file = out_path + EXTENSTIONS[file["mimeType"]]
                    file.GetContentFile(out_file, mimetype=mimetype)
                elif file["mimeType"] in (FOLDER_MINE, SHORTCUT_MINE):
                    pbar.update(1)
                    continue
                else:
                    try:
                        file.GetContentFile(out_path)
                    except Exception as e:
                        logger.error(file["title"], file["mimeType"])
                        print(e)
                pbar.update(1)

    def execute(self, *args):
        pass


if __name__ == "__main__":
    conf = {
        "setting_yml": "/Users/thucpk/IdeaProjects/data-warehouse/data-collector/"
                       "config/gsuide/gdrive/settings_local.yaml",
        "refresh_token": False,
    }
    gdrive_utils = GDriveUtils(config=conf)
    # dct_query = {"q": "modifiedDate > '2020-07-01T00:00:00' AND trashed=false"}
    # o_dir = "/Users/thucpk/IdeaProjects/onpoint/onboard/data/drive"
    # dct_folder_path = gdrive_utils.refresh_folder(dct_query, o_dir)
    # gdrive_utils.down_load_files_match(dct_query, dct_folder_path, o_dir)
    dct_query = {"q": "modifiedDate > '2020-07-01T00:00:00' AND title ='Target data for BI' AND"
                      "trashed=false"}
    gdrive_utils.down_load_files_match(dct_query, {}, "/tmp/")
