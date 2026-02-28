use std::fs::File;
use std::io::{BufWriter, Seek, SeekFrom, Write};

pub trait EncoderOutput {
    fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), String>;
    fn patch_bytes_at(&mut self, position: u64, bytes: &[u8]) -> Result<(), String>;
    fn current_byte_position(&mut self) -> Result<u64, String>;
}

pub struct FileEncoderOutput {
    writer: BufWriter<File>,
}

impl FileEncoderOutput {
    pub fn open_for_writing(path: &str) -> Result<Self, String> {
        let file = File::create(path)
            .map_err(|err| format!("cannot create output file '{path}': {err}"))?;
        Ok(Self {
            writer: BufWriter::with_capacity(8 * 1024 * 1024, file),
        })
    }
}

impl EncoderOutput for FileEncoderOutput {
    fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.writer
            .write_all(bytes)
            .map_err(|err| format!("write error: {err}"))
    }

    fn patch_bytes_at(&mut self, position: u64, bytes: &[u8]) -> Result<(), String> {
        self.writer
            .flush()
            .map_err(|err| format!("flush error: {err}"))?;
        let resume_position = self
            .writer
            .stream_position()
            .map_err(|err| format!("position error: {err}"))?;
        self.writer
            .seek(SeekFrom::Start(position))
            .map_err(|err| format!("seek error: {err}"))?;
        self.writer
            .write_all(bytes)
            .map_err(|err| format!("patch write error: {err}"))?;
        self.writer
            .seek(SeekFrom::Start(resume_position))
            .map_err(|err| format!("seek-resume error: {err}"))?;
        Ok(())
    }

    fn current_byte_position(&mut self) -> Result<u64, String> {
        self.writer
            .flush()
            .map_err(|err| format!("flush error: {err}"))?;
        self.writer
            .stream_position()
            .map_err(|err| format!("position error: {err}"))
    }
}

impl EncoderOutput for Vec<u8> {
    fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.extend_from_slice(bytes);
        Ok(())
    }

    fn patch_bytes_at(&mut self, position: u64, bytes: &[u8]) -> Result<(), String> {
        let start = position as usize;
        let end = start + bytes.len();
        self.get_mut(start..end)
            .ok_or_else(|| format!("patch_bytes_at: range {start}..{end} out of bounds"))?
            .copy_from_slice(bytes);
        Ok(())
    }

    fn current_byte_position(&mut self) -> Result<u64, String> {
        Ok(self.len() as u64)
    }
}
