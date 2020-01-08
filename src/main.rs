use std::env;
use std::collections::HashMap;
use std::cmp::Ordering;
use std::iter::FromIterator;

use serde::Deserialize;

use futures_util::stream::{StreamExt, TryStreamExt, futures_unordered::FuturesUnordered};

use hyper::{Body, Client, Request};
use hyper_tls::HttpsConnector;

use mysql_async::{params, Value, Pool, prelude::Queryable};

use once_cell::sync::Lazy;

use geo::{Point, Polygon};

use geo_raycasting::RayCasting;

use log::{error, info};

static POOL: Lazy<Pool> = Lazy::new(|| Pool::new(env::var("DATABASE_URL").expect("Missing DATABASE_URL env var")));

fn cmp_f64(a: &f64, b: &f64) -> Ordering {
    if a < b {
        Ordering::Less
    }
    else if a > b {
        Ordering::Greater
    }
    else {
        Ordering::Equal
    }
}

#[derive(Deserialize)]
struct OverpassResponse {
    pub elements: Vec<OverpassElement>,
}

impl OverpassResponse {
    fn into_params(self, city: &City) -> Vec<Vec<(String, Value)>> {
        let (nodes, elements): (Vec<OverpassElement>, Vec<OverpassElement>) = self.elements.into_iter().partition(|e| e.etype == "node");
        let mut coords = HashMap::new();
        nodes.into_iter().for_each(|node| match (node.lat, node.lon) {
            (Some(x), Some(y)) => {
                coords.insert(node.id, (x, y));
            },
            _ => {},
        });
        elements.into_iter()
            .filter(|element| element.nodes.as_ref().map(|nodes| nodes.iter().any(|id| coords.get(&id).map(|xy| city.coordinates.within(&Point::from(*xy))) == Some(true))) == Some(true))
            .map(|element| params! {
                "id" => element.id,
                "city_id" => city.id,
                "min_x" => element.get_min_x(&coords),
                "min_y" => element.get_min_y(&coords),
                "max_x" => element.get_max_x(&coords),
                "max_y" => element.get_max_y(&coords),
                "tags" => element.tags.map(|hm| hm.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<String>>().join("\n")),
                "coordinates" => element.nodes.map(|n| n.into_iter().map(|id| coords.get(&id).map(|(x, y)| format!("({},{})", x, y)).unwrap()).collect::<Vec<String>>().join(",")).unwrap_or_else(String::new),
            }).collect()
    }
}

#[derive(Deserialize)]
struct OverpassElement {
    #[serde(rename = "type")]
    pub etype: String,
    pub id: u64,
    pub nodes: Option<Vec<u64>>,
    pub tags: Option<HashMap<String, String>>,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
}

impl OverpassElement {
    fn get_min_x(&self, coords: &HashMap<u64, (f64, f64)>) -> Option<f64> {
        self.nodes.as_ref()
            .and_then(|n| {
                n.iter()
                    .map(|id| coords.get(&id).map(|(x, _)| *x))
                    .filter(Option::is_some)
                    .map(Option::unwrap)
                    .min_by(cmp_f64)
            })
    }

    fn get_min_y(&self, coords: &HashMap<u64, (f64, f64)>) -> Option<f64> {
        self.nodes.as_ref()
            .and_then(|n| {
                n.iter()
                    .map(|id| coords.get(&id).map(|(_, y)| *y))
                    .filter(Option::is_some)
                    .map(Option::unwrap)
                    .min_by(cmp_f64)
            })
    }

    fn get_max_x(&self, coords: &HashMap<u64, (f64, f64)>) -> Option<f64> {
        self.nodes.as_ref()
            .and_then(|n| {
                n.iter()
                    .map(|id| coords.get(&id).map(|(x, _)| *x))
                    .filter(Option::is_some)
                    .map(Option::unwrap)
                    .max_by(cmp_f64)
            })
    }

    fn get_max_y(&self, coords: &HashMap<u64, (f64, f64)>) -> Option<f64> {
        self.nodes.as_ref()
            .and_then(|n| {
                n.iter()
                    .map(|id| coords.get(&id).map(|(_, y)| *y))
                    .filter(Option::is_some)
                    .map(Option::unwrap)
                    .max_by(cmp_f64)
            })
    }
}

struct City {
    pub id: u16,
    pub name: String,
    pub coordinates: Polygon<f64>,
}

async fn load_cities(city_ids: Vec<String>) -> Result<HashMap<u16, City>, ()> {
    let query = format!(
            "SELECT id, name, coordinates FROM city WHERE scadenza > UNIX_TIMESTAMP(){}",
            if city_ids.is_empty() { String::new() } else { format!(" AND id IN ({})", city_ids.join(",")) }
        );
    let conn = POOL.get_conn().await.map_err(|e| error!("MySQL retrieve connection error: {}", e))?;
    let res = conn.query(&query).await.map_err(|e| error!("MySQL query error: {}", e))?;

    let mut cities = HashMap::new();
    res.for_each_and_drop(|ref mut row| {
        let id = row.take("id").expect("MySQL city.id error");
        let name = row.take("name").expect("MySQL city.name error");
        let coords = row.take::<String, _>("coordinates").expect("MySQL city.coordinates encoding error");
        let coords = coords.replace(char::is_whitespace, "");

        let poly: Vec<Point<f64>> = if coords.is_empty() {
            error!("City \"{}\" ({}) has empty coordinates", name, id);
            Vec::new()
        }
        else {
            (&coords[1..(coords.len() - 2)]).split("),(")
                .map(|s| {
                    let x_y: Vec<f64> = s.split(",")
                        .map(|s| match s.parse::<f64>() {
                            Ok(f) => f,
                            Err(_) => panic!("Error parsing \"{}\" as a float", s),
                        })
                        .collect();
                    if x_y.len() == 2 {
                        Some(Point::new(x_y[0], x_y[1]))
                    }
                    else {
                        error!("City \"{}\" ({}) has invalid coordinates", name, id);
                        None
                    }
                })
                .filter(Option::is_some)
                .map(Option::unwrap)
                .collect()
        };

        cities.insert(id, City {
            id,
            name,
            coordinates: Polygon::new(poly.into(), vec![]),
        });
    }).await.map_err(|e| error!("MySQL for_each error: {}", e))?;

    Ok(cities)
}

async fn load_parks<I: Iterator<Item=(u16, City)>>(cities: I) -> Result<(), ()> {
    let https = HttpsConnector::new();
    let client = Client::builder().build::<_, Body>(https);
    FuturesUnordered::from_iter(cities.map(|(_, city)| match (
                    city.coordinates.exterior().points_iter().map(|p| p.x()).min_by(cmp_f64),
                    city.coordinates.exterior().points_iter().map(|p| p.x()).max_by(cmp_f64),
                    city.coordinates.exterior().points_iter().map(|p| p.y()).min_by(cmp_f64),
                    city.coordinates.exterior().points_iter().map(|p| p.y()).max_by(cmp_f64)
                ) {
                (Some(min_x), Some(max_x), Some(min_y), Some(max_y)) => Some((city, (min_y, min_x, max_y, max_x))),
                _ => {
                    error!("Skipping city {}", city.name);
                    None
                },
            })
            .filter(Option::is_some)
            .map(Option::unwrap)
            .map(|(city, (min_y, min_x, max_y, max_x))| {
                let client = &client;
                let body = format!(r#"data=[out:json][bbox:{min_x},{min_y},{max_x},{max_y}];
(
    way["leisure"="park"];
    way["leisure"="recreation_ground"];
    way["leisure"="pitch"];
    way["leisure"="playground"];
    way["leisure"="golf_course"];
    way["landuse"="recreation_ground"];
    way["landuse"="meadow"];
    way["landuse"="grass"];
);
out body;
>;
out skel qt;"#, min_y = min_y, min_x = min_x, max_y = max_y, max_x = max_x);
                async move {
                    match Request::builder()
                            .method("POST")
                            // .header("User-Agent", "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:71.0) Gecko/20100101 Firefox/71.0")
                            .uri("https://lz4.overpass-api.de/api/interpreter")
                            .body(Body::from(body)) {
                        Ok(req) => (city, client.request(req).await),
                        Err(e) => panic!("error building request: {}", e),
                    }
                }
            }))
        .for_each_concurrent(None, |(city, res)| async move {
            if let Err(e) = res {
                error!("error retrieving parks for city \"{}\" ({}): {}", city.name, city.id, e);
                return;
            }
            let res = res.unwrap();
            if !res.status().is_success() {
                error!("unsuccessful response while retrieving parks for city \"{}\" ({})", city.name, city.id);
            }
            else {
                let body = match res.into_body()
                        .map_ok(|c| c.to_vec())
                        .try_concat()
                        .await
                        .map_err(|e| error!("error while reading parks for city \"{}\" ({}): {}", city.name, city.id, e))
                        .and_then(|chunks| String::from_utf8(chunks).map_err(|e| error!("error while encoding parks for city \"{}\" ({}): {}", city.name, city.id, e))) {
                    Ok(s) => s,
                    Err(_) => {
                        return;
                    },
                };

                let json: OverpassResponse = match serde_json::from_str(&body) {
                    Ok(json) => json,
                    Err(e) => {
                        error!("error decoding parks for city \"{}\" ({}): {}", city.name, city.id, e);
                        return;
                    }
                };

                let parks = json.into_params(&city);
                info!("Found {} parks for city \"{}\" ({})", parks.len(), city.name, city.id);

                match POOL.get_conn().await.map_err(|e| error!("MySQL retrieve connection error: {}", e)) {
                    Ok(conn) => {
                        conn.batch_exec("INSERT INTO city_parks (id, city_id, min_x, min_y, max_x, max_y, coordinates, tags)
                            VALUES (:id, :city_id, :min_x, :min_y, :max_x, :max_y, :coordinates, :tags)
                            ON DUPLICATE KEY UPDATE city_id = :city_id, min_x = :min_x, min_y = :min_y, max_x = :max_x, max_y = :max_y, coordinates = :coordinates, tags = :tags", parks).await
                            .map_err(|e| error!("MySQL insert query error: {}", e))
                            .ok();
                    },
                    Err(_) => {},
                }
            }
        })
        .await;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), ()> {
    env_logger::init();

    let city_ids: Vec<String> = env::args().skip(1).collect();

    let cities = load_cities(city_ids).await?;

    load_parks(cities.into_iter()).await
}
