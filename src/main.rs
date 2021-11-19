use std::fs;
use std::io;
use std::io::Write;
use std::path::Path;

use rusqlite::{params, Statement, Connection};
use sha2::{Digest, Sha512};

fn get_input() -> String {
	let mut input = String::new();
	io::stdin().read_line(&mut input).unwrap();

	// Remove whitespace
	input.retain(|c| !c.is_whitespace());

	input
}

fn get_hash(path: &Path) -> String {
	let mut hasher = Sha512::new();
	let mut file = fs::File::open(path).unwrap();

	io::copy(&mut file, &mut hasher).unwrap();
	let hash_u8 = hasher.finalize();
	
	format!("{:x}", hash_u8)
}

fn process_file(path: &Path, stmt: &mut Statement) {
	stmt.execute(params![path.to_str().unwrap(), get_hash(path)]).unwrap();
}

fn iterate_directory(path: &Path, stmt: &mut Statement) {
	for entry in fs::read_dir(path).unwrap() {
		let entry = entry.unwrap();
		let path = entry.path();

		if path.is_dir() {
			iterate_directory(&path, stmt);
		} else {
			process_file(&path, stmt);
		}
	}
}

fn get_duplicates(conn: &Connection) -> Vec<(String, usize)> {
	let mut duplicates: Vec<(String, usize)> = Vec::new();
	let mut stmt = conn.prepare("SELECT sha512, COUNT(*) c FROM files GROUP BY sha512 HAVING c > 1").unwrap();
	let mut rows = stmt.query([]).unwrap();

	while let Some(row) = rows.next().unwrap() {
		let hash: String = row.get(0).unwrap();
		let count: usize = row.get(1).unwrap();
		duplicates.push((hash, count));
	}

	duplicates
}

fn view_duplicates(conn: &Connection) {
	let duplicates = get_duplicates(conn);
	let mut stmt = conn.prepare("SELECT path FROM files WHERE sha512 = ?1").unwrap();

	for (hash, _count) in duplicates {
		println!("{}",&hash[..64]);
		let mut rows = stmt.query([hash]).unwrap();

		while let Some(row) = rows.next().unwrap() {
			let path: String = row.get(0).unwrap();
			println!("\t- {}", path);
		}
	}
}

fn remove_duplicates(conn: &Connection) {
	let duplicates = get_duplicates(conn);
	let mut stmt = conn.prepare("SELECT path FROM files WHERE sha512 = ?1").unwrap();

	for (hash, count) in duplicates {
		println!("{}",&hash[..64]);
		let mut rows = stmt.query([hash]).unwrap();

		let mut files = Vec::with_capacity(count);

		let mut i = 0;
		while let Some(row) = rows.next().unwrap() {
			let path: String = row.get(0).unwrap();
			println!("\t{}: {}", i, &path);

			i = i+1;
			files.push(path);
		}

		loop {
			print!("Which file would you like to keep? (or 'skip')\n> ");
			io::stdout().flush().unwrap();
			let input = get_input();
			if input == "skip" {
				break;
			}

			let select = input.parse::<usize>();
			if select.is_ok() {
				let select = select.unwrap();
				if select < i {
					for j in 0..i {
						if j != select {
							fs::remove_file(Path::new(&files[j])).unwrap();
						}
					}
					break;
				}
			}
			
			println!("Invalid input");
		}
	}
}

fn main() {
	let conn = Connection::open("dedupe.db3").unwrap();
	conn.execute("CREATE TABLE files (path TEXT, sha512 TEXT)", []).unwrap();
	let mut stmt = conn.prepare("INSERT INTO files (path, sha512) VALUES (?1, ?2)").unwrap();

	let arg: &str = &std::env::args().nth(1).expect("No argument given");
	let path = std::env::args().nth(2);

	match arg {
		"--scan" => {
			conn.execute("DROP TABLE IF EXISTS files", []).unwrap();
			iterate_directory(&Path::new(&path.expect("No path given")), &mut stmt)
		},
		"--view" => view_duplicates(&conn),
		"--dedupe" => remove_duplicates(&conn),
		_ => println!("Invalid argument"),
	}
}
