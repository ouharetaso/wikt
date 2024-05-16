use rusqlite::{Connection, params, Transaction};
use std::fs::File;
use std::io::*;
use bzip2::read::BzDecoder;
use regex::Regex;

#[allow(dead_code)]

pub struct MultistreamIndex {
    bz2_offset: usize,
    id: u32,
    title: String,
}


pub struct WiktionaryDump {
    pub multistream_path: String,
    pub con: Connection
}


impl WiktionaryDump{
    pub fn new(multistream_path: &str, index_db_path: &str) -> WiktionaryDump {
        WiktionaryDump {
            multistream_path: multistream_path.to_string(),
            con: Connection::open(index_db_path).expect("cannot open db")
        }
    }

    #[allow(dead_code)]
    pub fn make_multistream_index(&mut self, index_bz2_path: &str) {    
        self.con.execute(
        "CREATE TABLE multistream_index (
                bz2_offset  INTEGER,
                id          INTEGER,
                title       TEXT
            )",
         ()).unwrap();
        
        let stream_index_file = File::open(index_bz2_path).expect("cannot open multistream index bz2 file");
    
        let mut decoder = BzDecoder::new(BufReader::new(stream_index_file));
        let mut decoded_data = String::new();
    
        decoder.read_to_string(&mut decoded_data).unwrap();
    
        let lines = decoded_data.lines();
    
        let tx = self.con.transaction().unwrap();
    
        for line in lines {
            let parts: Vec<&str> = line.split(":").collect();
            if parts.len() == 3 {
                let index: MultistreamIndex = MultistreamIndex {
                    bz2_offset: parts[0].parse::<usize>().unwrap(),
                    id: parts[1].parse::<u32>().unwrap(), 
                    title: parts[2].to_string()
                };
    
                insert_db(&tx, &index);
            }
        }
    
        tx.commit().unwrap();

        self.con.execute(
            "CREATE INDEX idx_title ON multistream_index(title);
                CREATE INDEX idx_bz2_offset ON multistream_index(bz2_offset);",
             ()).unwrap();
        }

    pub fn get_raw_article(&self, title: &str) -> Option<String> {
        let offset_start = self.get_article_offset(title)?;

        let mut multistream = File::open(self.multistream_path.as_str()).expect("cannot open multistream bz2 file");
        multistream.seek(SeekFrom::Start(offset_start as u64)).unwrap();

        let mut decoder = BzDecoder::new(multistream);
        let mut pages = String::new();
        decoder.read_to_string(&mut pages).unwrap();

        let re_title = Regex::new(&format!(r"<title>{}</title>", title)).unwrap();

        if let Some(title_match) = re_title.find(&pages) {
            let re_text_tag_start = Regex::new(r"<text [^>]+?>").unwrap();
            let re_text_tag_end = Regex::new(r"</text>").unwrap();
            let raw_article_start = re_text_tag_start.find_at(&pages, title_match.end())?.end();
            let raw_article_end = re_text_tag_end.find_at(&pages, raw_article_start)?.start();

            Some(pages[raw_article_start..raw_article_end].to_string())
        }
        else {
            None
        }
    }

    fn get_article_offset(&self, title: &str) -> Option<usize>{
        let mut stmt = self.con.prepare("SELECT bz2_offset, id FROM multistream_index WHERE title = ?1").unwrap();

        if let Ok(result )= stmt.query_row(params![title], |row| {
            let bz2_offset: usize = row.get(0)?;
            Ok(bz2_offset)
        }){
            Some(result)
        }
        else{
            None
        }
    }
}


fn insert_db(tx: &Transaction<'_>, index: &MultistreamIndex) {
    tx.execute(
        "INSERT INTO multistream_index (bz2_offset, id, title) VALUES (?1, ?2, ?3)",
        params![index.bz2_offset, index.id, index.title]
    ).unwrap();
}


#[cfg(test)]
mod tests{
    use super::*;

    #[test]
    fn test_get_article_offset() {
        let wikt_dump = WiktionaryDump::new("enwiktionary-latest-pages-articles-multistream.xml.bz2",
        "multistream_index.db");

        let title = "ungefähr";

        let expected_offset_start: usize = 215051411;

        let result = wikt_dump.get_article_offset(title);

        assert!(result.is_some()); 

        let offset_start = result.unwrap();

        assert_eq!(offset_start, expected_offset_start);
    }

    #[test]
    fn test_get_raw_article(){
        let wikt_dump = WiktionaryDump::new("enwiktionary-latest-pages-articles-multistream.xml.bz2",
        "multistream_index.db");

        let title = "ungefähr";

        wikt_dump.get_raw_article(title);
    }
}