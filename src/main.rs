use std::env;

mod wiktionary_dump;
use wiktionary_dump::*;


fn main() {
    let args: Vec<String> = env::args().collect();

    let wikt_dump = WiktionaryDump::new("enwiktionary-latest-pages-articles-multistream.xml.bz2",
                                "multistream_index.db");

    if args.len() >= 2 {
        let word = &args[1];

        if let Some(article) = wikt_dump.get_raw_article(word) {
            print!("{}", article);
        }
        else {
            println!("Word {} not found", word);
        }    
    }
    else {
        println!("specify word");
    }
    

}
