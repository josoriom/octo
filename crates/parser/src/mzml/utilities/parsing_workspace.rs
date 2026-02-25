use crate::mzml::schema::TagId;
use crate::mzml::utilities::{
    ParamCollector, drain_until_close, read_ref_group_ref, read_software_param, read_user_param,
    tag_id_from_bytes,
};
use crate::mzml::utilities::{ParseError, read_cv_param};
use quick_xml::Reader;
use quick_xml::events::{BytesStart, Event};
use std::{io::BufRead, str::from_utf8_unchecked};

#[allow(dead_code)] // Coming soon!
#[derive(Clone, Debug, Copy, PartialEq, Eq, Default)]
pub(crate) enum StrictnessPolicy {
    #[default]
    Permissive,
    Warn,
    Error,
}

pub(crate) struct ParsingWorkspace<R> {
    pub(crate) xml_reader: Reader<R>,
    pool: BufferPool,
    pub(crate) policy: StrictnessPolicy,
    pub(crate) warnings: Vec<String>,
}

impl<R: BufRead> ParsingWorkspace<R> {
    pub(crate) fn new(mut xml_reader: Reader<R>) -> Self {
        xml_reader.config_mut().trim_text(true);
        Self {
            xml_reader,
            pool: BufferPool::new(),
            policy: StrictnessPolicy::default(),
            warnings: Vec::new(),
        }
    }

    fn read_one(&mut self) -> Result<Event<'static>, ParseError> {
        let reader = &mut self.xml_reader;
        self.pool.with_buf(|buf| {
            buf.clear();
            reader
                .read_event_into(buf)
                .map(|e| e.into_owned())
                .map_err(ParseError::from)
        })
    }

    pub(crate) fn next_event(&mut self) -> Result<Event<'static>, ParseError> {
        self.read_one()
    }

    pub(crate) fn for_each_child<F>(
        &mut self,
        start: &BytesStart<'_>,
        mut on_child: F,
    ) -> Result<(), ParseError>
    where
        F: FnMut(&mut Self, ChildEvent) -> Result<bool, ParseError>,
    {
        let closing: Vec<u8> = start.name().as_ref().to_vec();

        loop {
            let event = self.read_one()?;
            match event {
                Event::Start(e) => {
                    let tag = tag_id_from_bytes(e.name().as_ref());
                    let raw_name: Vec<u8> = e.name().as_ref().to_vec();
                    let handled = on_child(self, ChildEvent::Open(tag, e))?;
                    if !handled {
                        let tag_str = unsafe { from_utf8_unchecked(&raw_name) };
                        match self.policy {
                            StrictnessPolicy::Error => {
                                return Err(ParseError::UnexpectedTag {
                                    tag: tag_str.to_string(),
                                    byte_offset: self.xml_reader.buffer_position(),
                                });
                            }
                            StrictnessPolicy::Warn => self.warnings.push(format!(
                                "skipping unknown <{tag_str}> at byte {}",
                                self.xml_reader.buffer_position()
                            )),
                            StrictnessPolicy::Permissive => {}
                        }
                        drain_until_close(self, &raw_name)?;
                    }
                }
                Event::Empty(e) => {
                    let tag = tag_id_from_bytes(e.name().as_ref());
                    on_child(self, ChildEvent::SelfClosed(tag, e))?;
                }
                Event::End(e) if e.name().as_ref() == closing.as_slice() => break Ok(()),
                Event::Eof => {
                    let offset = self.xml_reader.buffer_position();
                    let ctx = unsafe { from_utf8_unchecked(&closing) }.to_string();
                    break Err(ParseError::UnexpectedEof {
                        context: ctx,
                        byte_offset: offset,
                    });
                }
                _ => {}
            }
        }
    }

    pub(crate) fn collect_params_into<T: ParamCollector>(
        &mut self,
        start: &BytesStart<'_>,
        target: &mut T,
    ) -> Result<(), ParseError> {
        self.for_each_child(start, |_ws, event| {
            let (tag, element, _) = event.into_parts();
            match tag {
                TagId::CvParam => target.receive_cv(read_cv_param(&element)),
                TagId::UserParam => target.receive_user(read_user_param(&element)),
                TagId::SoftwareParam => target.receive_software(read_software_param(&element)),
                TagId::ReferenceableParamGroupRef => {
                    target.receive_ref_group(read_ref_group_ref(&element))
                }
                _ => return Ok(false),
            }
            Ok(true)
        })
    }
}

struct BufferPool(Vec<Vec<u8>>);

impl BufferPool {
    fn new() -> Self {
        Self(vec![Vec::with_capacity(4096), Vec::with_capacity(4096)])
    }

    fn get(&mut self) -> Vec<u8> {
        self.0.pop().unwrap_or_else(|| Vec::with_capacity(4096))
    }

    fn put(&mut self, mut buf: Vec<u8>) {
        buf.clear();
        self.0.push(buf);
    }

    fn with_buf<F, T, E>(&mut self, f: F) -> Result<T, E>
    where
        F: FnOnce(&mut Vec<u8>) -> Result<T, E>,
    {
        let mut buf = self.get();
        let result = f(&mut buf);
        self.put(buf);
        result
    }
}

pub(crate) enum ChildEvent {
    Open(TagId, BytesStart<'static>),
    SelfClosed(TagId, BytesStart<'static>),
}

impl ChildEvent {
    pub(crate) fn into_parts(self) -> (TagId, BytesStart<'static>, bool) {
        match self {
            Self::Open(tag, e) => (tag, e, true),
            Self::SelfClosed(tag, e) => (tag, e, false),
        }
    }
}
