from collections import ChainMap
from datetime import datetime
import json
import types

import boto3
from satstac import Catalog as _Catalog, Item

SOURCE_BUCKET_NAME = "radiant-nasa-iserv"
TARGET_BUCKET_NAME = "iserv-stac"


class Catalog(_Catalog):
    def save(self):
        pass


s3 = boto3.resource("s3")
source_bucket = s3.Bucket(SOURCE_BUCKET_NAME)
target_bucket = s3.Bucket(TARGET_BUCKET_NAME)

root_description = """
# ISS SERVIR Environmental Research and Visualization System (ISERV) Level-0 Product

## Introduction

These products were created at the ISERV Science Operation Center (SOC) at
the National Space Science and Technology Center.

ISERV is an automated system designed to acquire images of the Earth's
surface from the International Space Station (ISS). It is primarily a means
to gain experience and expertise in automated data acquisition from the ISS,
although it is expected to provide useful images for use in disaster
monitoring and assessment, and environmental decision making.

For more information about ISERV:
http://www.nasa.gov/mission_pages/servir/index.html

For more information about SERVIR:
http://servirglobal.net

## Format

ISERV data is provided in JPEG format for Level 0 products. Level 0 is
georeferenced using the following coordinate system:
`WGS_1984_Web_Mercator_Auxiliary_Sphere`

GeoTIFFs are provided by ISERV. Cloud-optimized GeoTIFFs are provided by
Radiant Earth Foundation

## Organization

ISERV is a true color image in JPEG format. Each band is delivered as a
grayscale, JPEG-compressed, 8-bit string of unsigned integers. Bands are not
calibrated. Accompanying each image is an XML auxiliary file, a JGW ("world")
file that provides image location and geometry information, and an OVR
overview file containing the reduced resolution image pyramid for ease of use
in image processing software.

### Data File Names

The file naming convention for ISERV L-0 data is as follows:

`IP0YYMMDDhhmmssLATLON.JPG` where:

* `IP` - ISERV Pathfinder
* `0` - Processing level
* `YY` - two-numeral year
* `MM` - two-numeral month
* `DD` - two-numeral day
* `hh` - two-numeral hour
* `mm` - two-numeral minute
* `ss` - two-numeral second
* `LAT` - four-numeral latitude in decimal degrees plus hemispherical indicator
  (`N` or `S`)
* `LON` - five-numeral longitude in decimal degrees plus hemispherical
  indicator (`E` or `W`)

Example: `IP01306111234561234N12345W.jpg`

`LON` is always 5 characters -- 3 left of decimal, 2 right of decimal, plus a
cardinal direction indicator

`123.45W` (or `-123.45`) becomes `12345W`

`23.45E` becomes `02345E`

`5.5E` becomes `00550E`
 
`LAT` is always 4 characters -- 2 left of decimal, 2 right of decimal, plus a
cardinal direction indicator

`34.56S` (or `-34.56`) becomes `3456S`

`4.56N` becomes `0456N`

`6.6S` becomes `0660S`

### READING DATA

Any image display software can open the JPEG image files. 
No software is included on this product for viewing ISERV data.

## General Information and Documentation

### ISERV data access

USGS EarthExplorer at https://earthexplorer.usgs.gov

### Data restrictions

This is an experimental product derived from the ISERV Pathfinder system.
Data and metadata files may be freely distributed without restriction.

### Credits

ISERV, NASA

### Level 0 Data Processing Levels

Reconstructed, unprocessed ISERV instrument data at full resolution in JPEG
format, with all communications artifacts (e.g., synchronization frames,
communications headers, duplicate data) removed. Images are geolocated using
a custom-built, automated georeferencing process which provides an average
positional accuracy of approximately 2km.

## Disclaimer

Any use of trade, product, or firm names is for descriptive purposes only and
does not imply endorsement by the U.S. Government.
"""

root_prefix = "https://iserv-stac.s3.amazonaws.com/0.6.1/"
root_href = "{}catalog.json".format(root_prefix)
root_catalog = Catalog.create(id="ISERV", description=root_description, version="1.0.0")
root_catalog = Catalog(
    {
        "id": "ISERV",
        "title": "",
        "description": root_description,
        "version": "1.0.0",
        "license": "PDDL-1.0",
        "keywords": ["NASA", "ISERV", "ISS", "satellite"],
        "extent": {
            "spatial": [-180, -90, 180, 90],
            "temporal": ["2013-03-27T14:18:28Z", "2014-11-27T11:56:49Z"],
        },
        "providers": [
            {
                "name": "SERVIR",
                "url": "http://www.nasa.gov/mission_pages/servir/index.html",
                "roles": ["producer", "licensor"],
            },
            {
                "name": "Radiant Earth Foundation",
                "url": "https://www.radiant.earth/",
                "roles": ["processor", "host"],
            },
        ],
        "properties": {
            "eo:gsd": 5.6,
            "eo:platform": "ISS",
            "eo:instrument": "ISERV",
            "eo:bands": [
                {
                    "center_wavelength": 0.7,
                    "common_name": "red",
                },
                {
                    "center_wavelength": 0.55,
                    "common_name": "green",
                },
                {
                    "center_wavelength": 0.45,
                    "common_name": "blue",
                },
            ]
        },
    }
)
root_catalog.add_link("root", root_href)
root_catalog.add_link("self", root_href)

catalogs = {"root": root_catalog}

for key in source_bucket.objects.all():
    if (
        not key.key.endswith(".json")
        or key.key.endswith("catalog.json")
        or key.key.endswith("iserv.json")
        or key.key.endswith("product.json")
    ):
        continue

    parents = []
    path_components = key.key.split("/")[0:3]
    for component in path_components:
        catalog_id = "/".join(parents + [component])
        catalog = catalogs.get(catalog_id)

        if catalog is None:
            if len(parents) == 0:
                parent = catalogs["root"]
            else:
                parent = catalogs["/".join(parents)]

            if len(parents) == 0:
                descriptive_timestamp = catalog_id
            elif len(parents) == 1:
                descriptive_timestamp = datetime.strptime(catalog_id, "%Y/%m").strftime(
                    "%B %Y"
                )
            elif len(parents) == 2:
                descriptive_timestamp = datetime.strptime(
                    catalog_id, "%Y/%m/%d"
                ).strftime("%B %-d, %Y")

            catalog_href = "{}/catalog.json".format(component)
            catalog = Catalog(
                {
                    "id": catalog_id,
                    "description": "Imagery from {}".format(descriptive_timestamp),
                }
            )
            catalog.add_link("root", root_href)
            catalog.add_link("collection", root_href)
            catalog.add_link("parent", "../catalog.json")
            parent.add_link("child", catalog_href)

            catalogs[catalog_id] = catalog

        parents.append(component)

    try:
        obj = source_bucket.Object(key.key)
        old_item = json.loads(obj.get()["Body"].read().decode("utf-8"))

        assets = old_item["assets"]

        source_prefix = "https://{}.s3.amazonaws.com/{}".format(
            SOURCE_BUCKET_NAME, "/".join(key.key.split("/")[0:-1])
        )

        new_assets = {}

        if type(assets) is dict:
            original_tiff = assets["RGB Tif"]
            original_tiff["href"] = "{}/{}".format(source_prefix, original_tiff["href"])
            original_tiff["title"] = "RGB GeoTIFF"
            original_tiff["type"] = "image/vnd.stac.geotiff"
            original_tiff["eo:bands"] = [0, 1, 2]
            del original_tiff["name"]

            new_assets["original TIFF"] = original_tiff

            if "tiff world file" in assets:
                original_tiff_world = assets["tiff world file"]
                original_tiff_world["href"] = "{}/{}".format(
                    source_prefix, original_tiff_world["href"]
                )
                original_tiff_world["title"] = "RGB GeoTIFF world file"
                original_tiff_world[
                    "type"
                ] = (
                    "text/plain"
                )  # per https://www.loc.gov/preservation/digital/formats/fdd/fdd000287.shtml#sign
                del original_tiff_world["name"]

                new_assets["original TIFF world file"] = original_tiff_world

            jpeg = assets["RGB JPEG"]
            jpeg["href"] = "{}/{}".format(source_prefix, jpeg["href"])
            jpeg["title"] = jpeg["name"]
            jpeg["type"] = "image/jpeg"
            del jpeg["name"]

            new_assets["JPEG"] = jpeg

            jpeg_overviews = assets["jpg overview"]
            jpeg_overviews["href"] = "{}/{}".format(
                source_prefix, jpeg_overviews["href"]
            )
            jpeg_overviews["title"] = "JPEG overviews"
            jpeg_overviews["type"] = "image/tiff"
            del jpeg_overviews["name"]

            new_assets["JPEG overviews"] = jpeg_overviews

            jpeg_world = assets["jpeg world file"]
            jpeg_world["href"] = "{}/{}".format(source_prefix, jpeg_world["href"])
            jpeg_world["title"] = "JPEG world file"
            jpeg_world["type"] = "text/plain"
            del jpeg_world["name"]

            new_assets["JPEG world file"] = jpeg_world

            thumbnail = assets["thumbnail"]
            thumbnail["href"] = "{}/{}".format(source_prefix, thumbnail["href"])
            thumbnail["title"] = "Thumbnail"
            thumbnail["type"] = "image/png"
            del thumbnail["name"]

            new_assets["thumbnail"] = thumbnail

            visual = assets["cog"]
            visual["href"] = "{}/{}".format(source_prefix, visual["href"])
            visual["title"] = visual["name"]
            visual["type"] = "image/vnd.stac.geotiff; cloud-optimized=true"
            del visual["format"]
            del visual["name"]

            new_assets["visual"] = visual

        elif type(assets) is list:
            for asset in assets:
                if asset["href"].endswith(".TFW"):
                    new_assets["original TIFF world file"] = {
                        "href": "{}/{}".format(source_prefix, asset["href"]),
                        "title": "RGB GeoTIFF world file",
                        "type": "text/plain",
                    }

                if asset["href"].endswith(".JPG"):
                    new_assets["JPEG"] = {
                        "href": "{}/{}".format(source_prefix, asset["href"]),
                        "title": "RGB JPEG",
                        "type": "image/jpeg",
                    }

                if asset["href"].endswith(".png"):
                    new_assets["thumbnail"] = {
                        "href": "{}/{}".format(source_prefix, asset["href"]),
                        "title": "Thumbnail",
                        "type": "image/png",
                    }

                if asset["href"].endswith(".JGW"):
                    new_assets["JPEG world file"] = {
                        "href": "{}/{}".format(source_prefix, asset["href"]),
                        "title": "JPEG world file",
                        "type": "text/plain",
                    }

                if asset["href"].endswith(".JPG.ovr"):
                    new_assets["JPEG overviews"] = {
                        "href": "{}/{}".format(source_prefix, asset["href"]),
                        "title": "JPEG overviews",
                        "type": "image/tiff",
                    }

                if asset["href"].endswith(".TIF"):
                    new_assets["visual"] = {
                        "href": "{}/{}".format(source_prefix, asset["href"]),
                        "title": "3-Band RGB GeoTIFF",
                        "type": "image/vnd.stac.geotiff; cloud-optimized=true",
                    }

        item = Item(
            {
                "type": "Feature",
                "id": old_item["id"],
                "bbox": old_item["bbox"],
                "geometry": old_item["geometry"],
                "properties": {
                    "datetime": old_item["properties"].get(
                        "datetime", old_item["properties"].get("start")
                    )
                },
                "assets": new_assets,
            }
        )

        item_href = "{}.json".format(item.id)
        item.add_link("root", root_href)
        item.add_link("parent", "../catalog.json")
        item.add_link("self", "{}{}/{}".format(root_prefix, catalog_id, item_href))

        key = "0.6.1/{}/{}".format(catalog_id, item_href)

        print(key)
        obj = target_bucket.Object(key)
        obj.put(Body=json.dumps(item.data), ContentType="application/json")

        catalog.add_link("item", item_href, title=item.id)
    except Exception as e:
        print(key.key)
        del old_item["geometry"]
        print(json.dumps(old_item))
        raise e

for v in catalogs.values():
    element = dict(ChainMap({}, {"stac_version": "0.6.1"}, v.data))

    if v.id == "ISERV":
        key = "0.6.1/catalog.json"
    else:
        key = "0.6.1/{}/catalog.json".format(element["id"].replace("ISERV", ""))

    print(key)
    obj = target_bucket.Object(key)
    obj.put(Body=json.dumps(element), ContentType="application/json")
