import os
import shutil
from pathlib import Path
from xml.etree import ElementTree as ET

destination = Path("/tmp/hadoop-conf")
destination.mkdir(exist_ok=True)
for p in Path("/opt/hadoop/etc/hadoop").glob("*"):
    if p.is_file():
        shutil.copy2(p, destination / p.name)
for p in Path("/opt/base-conf").glob("*"):
    shutil.copy2(p, destination / p.name)
role = os.environ.get("ROLE_ID", "")
updates = {}
if role.startswith("nn"):
    updates["hdfs-site.xml"] = {"dfs.ha.namenode.id": role}
if role.startswith("dn"):
    updates["hdfs-site.xml"] = {"dfs.datanode.hostname": role}
if role.startswith("rm"):
    updates["yarn-site.xml"] = {"yarn.resourcemanager.ha.id": role}
if role.startswith("nm"):
    updates["yarn-site.xml"] = {"yarn.nodemanager.hostname": role}
for name, props in updates.items():
    tree = ET.parse(destination / name)
    for key, value in props.items():
        node = ET.SubElement(tree.getroot(), "property")
        ET.SubElement(node, "name").text = key
        ET.SubElement(node, "value").text = value
    tree.write(destination / name, encoding="utf-8", xml_declaration=True)
