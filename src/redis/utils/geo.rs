/* Taken from: https://github.com/codecrafters-io/redis-geocoding-algorithm/tree/main/rust */

const MIN_LATITUDE: f64 = -85.05112878;
const MAX_LATITUDE: f64 = 85.05112878;
const MIN_LONGITUDE: f64 = -180.0;
const MAX_LONGITUDE: f64 = 180.0;

const LATITUDE_RANGE: f64 = MAX_LATITUDE - MIN_LATITUDE;
const LONGITUDE_RANGE: f64 = MAX_LONGITUDE - MIN_LONGITUDE;

pub fn validate_latlon(lat: f64, lon: f64) -> bool {
    (lat >= MIN_LATITUDE && lat <= MAX_LATITUDE) && (lon >= MIN_LONGITUDE && lon <= MAX_LONGITUDE)
}

pub fn encode_latlon(lat: f64, lon: f64) -> u64 {
    let normalised_lat = (2.0_f64.powi(26) * (lat - MIN_LATITUDE) / LATITUDE_RANGE) as u32;
    let normalised_lon = (2.0_f64.powi(26) * (lon - MIN_LONGITUDE) / LONGITUDE_RANGE) as u32;

    interleave(normalised_lat, normalised_lon)
}

pub fn decode_latlon(score: u64) -> (f64, f64) {
    let y = score >> 1;
    let x = score;

    let grid_lat = compact_int64_to_int32(x);
    let grid_lon = compact_int64_to_int32(y);

    convert_grid_numbers_to_coordinates(grid_lat, grid_lon)
}

/* Encoding */

fn spread_int32_to_int64(v: u32) -> u64 {
    let mut result = v as u64;
    result = (result | (result << 16)) & 0x0000FFFF0000FFFF;
    result = (result | (result << 8)) & 0x00FF00FF00FF00FF;
    result = (result | (result << 4)) & 0x0F0F0F0F0F0F0F0F;
    result = (result | (result << 2)) & 0x3333333333333333;
    (result | (result << 1)) & 0x5555555555555555
}

fn interleave(x: u32, y: u32) -> u64 {
    let x_spread = spread_int32_to_int64(x);
    let y_spread = spread_int32_to_int64(y);
    let y_shifted = y_spread << 1;
    x_spread | y_shifted
}

/* End Encoding */

/* Decoding */

fn compact_int64_to_int32(v: u64) -> u32 {
    let mut result = v & 0x5555555555555555;
    result = (result | (result >> 1)) & 0x3333333333333333;
    result = (result | (result >> 2)) & 0x0F0F0F0F0F0F0F0F;
    result = (result | (result >> 4)) & 0x00FF00FF00FF00FF;
    result = (result | (result >> 8)) & 0x0000FFFF0000FFFF;
    ((result | (result >> 16)) & 0x00000000FFFFFFFF) as u32 // Cast to u32
}

fn convert_grid_numbers_to_coordinates(
    grid_latitude_number: u32,
    grid_longitude_number: u32,
) -> (f64, f64) {
    let grid_latitude_min =
        MIN_LATITUDE + LATITUDE_RANGE * (grid_latitude_number as f64 / 2.0_f64.powi(26));
    let grid_latitude_max =
        MIN_LATITUDE + LATITUDE_RANGE * ((grid_latitude_number + 1) as f64 / 2.0_f64.powi(26));
    let grid_longitude_min =
        MIN_LONGITUDE + LONGITUDE_RANGE * (grid_longitude_number as f64 / 2.0_f64.powi(26));
    let grid_longitude_max =
        MIN_LONGITUDE + LONGITUDE_RANGE * ((grid_longitude_number + 1) as f64 / 2.0_f64.powi(26));

    let latitude = (grid_latitude_min + grid_latitude_max) / 2.0;
    let longitude = (grid_longitude_min + grid_longitude_max) / 2.0;

    (latitude, longitude)
}

/* End Decoding */
