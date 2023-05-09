use std::error::Error;
use std::ffi::OsString;
/// This file contains some helper code to create SVG images from time DAGs to show whats going on
/// in a document.
///
/// It was mostly made as an aide to debugging. Compilation is behind a feature flag (dot_export)

use std::fmt::{Display, Formatter};
use std::io::Write as _;
use std::process::{Command, Stdio};

// pub fn name_of(time: LV) -> String {
//     if time == LV::MAX { panic!("Should not see ROOT_TIME here"); }
//
//     format!("{}", time)
// }

#[derive(Debug)]
struct DotError;

impl Display for DotError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("dot command failed with an error")
    }
}

impl Error for DotError {}

pub fn generate_svg_with_dot(dot_content: String, dot_path: Option<OsString>) -> Result<String, Box<dyn Error>> {
    let dot_path = dot_path.unwrap_or_else(|| "dot".into());
    let mut child = Command::new(dot_path)
        // .arg("-Tpng")
        .arg("-Tsvg")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    let mut stdin = child.stdin.take().unwrap();
    // Spawn is needed here to prevent a potential deadlock. See:
    // https://doc.rust-lang.org/std/process/index.html#handling-io
    std::thread::spawn(move || {
        stdin.write_all(dot_content.as_bytes()).unwrap();
    });

    let out = child.wait_with_output()?;

    // Pipe stderr.
    std::io::stderr().write_all(&out.stderr)?;

    if out.status.success() {
        Ok(String::from_utf8(out.stdout)?)
    } else {
        // May as well pipe stdout too.
        std::io::stdout().write_all(&out.stdout)?;
        Err(DotError.into())
    }
}
