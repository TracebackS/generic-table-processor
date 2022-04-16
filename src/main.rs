use std::error::Error;
use std::io;

mod data_represent;

fn main() -> Result<(), Box<dyn Error>> {
    let mut rdr = csv::Reader::from_reader(io::stdin());
    for e in rdr.records() {
        let record = e?;
        println!("{:?}", record);
    }
    return Ok(());
}
