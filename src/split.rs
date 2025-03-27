use std::fs::File;
use std::io::{self, BufRead, BufReader, Write}; // 添加 BufReader
use std::path::Path;

pub fn split_file(input_path: &str, lines_per_file: usize) -> Result<(), io::Error> {
    let file = File::open(input_path)?;
    let reader = BufReader::new(file);

    let mut current_file_index = 1;
    let mut current_line_count = 0;
    let mut current_writer: Option<File> = None;
    let base_name = Path::new(input_path)
        .file_stem()
        .unwrap()
        .to_str()
        .unwrap();
    let ext = Path::new(input_path)
        .extension()
        .map_or("", |e| e.to_str().unwrap());

    for line in reader.lines() {
        let line = line?;
        if current_writer.is_none() || current_line_count >= lines_per_file {
            let output_path = format!("{}_part_{}.{}", base_name, current_file_index, ext);
            current_writer = Some(File::create(&output_path)?);
            current_line_count = 0;
            current_file_index += 1;
        }
        if let Some(ref mut writer) = current_writer {
            writeln!(writer, "{}", line)?;
            current_line_count += 1;
        }
    }

    println!("文件分割完成，生成了 {} 个文件", current_file_index - 1);
    Ok(())
}