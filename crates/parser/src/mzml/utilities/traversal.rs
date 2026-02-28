use crate::mzml::structs::*;

#[allow(dead_code)] // New feature!
pub enum ParamEvent {
    Cv(CvParam),
    User(UserParam),
    Software(SoftwareParam),
    Ref(ReferenceableParamGroupRef),
}

pub trait ParamCollector {
    fn receive_cv(&mut self, param: CvParam);
    fn receive_user(&mut self, param: UserParam);
    fn receive_ref_group(&mut self, _param: ReferenceableParamGroupRef) {}
    fn receive_software(&mut self, _param: SoftwareParam) {}
}

macro_rules! impl_param_collector {
    ($ty:ty { cv: $cv:ident, user: $user:ident, ref: $r:ident }) => {
        impl ParamCollector for $ty {
            fn receive_cv(&mut self, p: CvParam) {
                self.$cv.push(p);
            }
            fn receive_user(&mut self, p: UserParam) {
                self.$user.push(p);
            }
            fn receive_ref_group(&mut self, p: ReferenceableParamGroupRef) {
                self.$r.push(p);
            }
        }
    };
    ($ty:ty { cv: $cv:ident, user: $user:ident, ref_opt: $r:ident }) => {
        impl ParamCollector for $ty {
            fn receive_cv(&mut self, p: CvParam) {
                self.$cv.push(p);
            }
            fn receive_user(&mut self, p: UserParam) {
                self.$user.push(p);
            }
            fn receive_ref_group(&mut self, p: ReferenceableParamGroupRef) {
                self.$r = Some(p);
            }
        }
    };
    ($ty:ty { cv: $cv:ident, user: $user:ident }) => {
        impl ParamCollector for $ty {
            fn receive_cv(&mut self, p: CvParam) {
                self.$cv.push(p);
            }
            fn receive_user(&mut self, p: UserParam) {
                self.$user.push(p);
            }
        }
    };
    ($ty:ty { cv: $cv:ident, user: $user:ident, sw: $sw:ident }) => {
        impl ParamCollector for $ty {
            fn receive_cv(&mut self, p: CvParam) {
                self.$cv.push(p);
            }
            fn receive_user(&mut self, p: UserParam) {
                self.$user.push(p);
            }
            fn receive_software(&mut self, p: SoftwareParam) {
                self.$sw.push(p);
            }
        }
    };
}

impl_param_collector!(FileContent      { cv: cv_params, user: user_params,  ref: referenceable_param_group_refs     });
impl_param_collector!(SourceFile       { cv: cv_param,  user: user_param,   ref: referenceable_param_group_ref      });
impl_param_collector!(Contact          { cv: cv_params, user: user_params,  ref: referenceable_param_group_refs     });
impl_param_collector!(ReferenceableParamGroup {
    cv: cv_params,
    user: user_params
});
impl_param_collector!(Sample {
    cv: cv_params,
    user: user_params,
    ref_opt: referenceable_param_group_ref
});
impl_param_collector!(Instrument       { cv: cv_param,  user: user_param,   ref: referenceable_param_group_ref      });
impl_param_collector!(Source           { cv: cv_param,  user: user_param,   ref: referenceable_param_group_ref      });
impl_param_collector!(Analyzer         { cv: cv_param,  user: user_param,   ref: referenceable_param_group_ref      });
impl_param_collector!(Detector         { cv: cv_param,  user: user_param,   ref: referenceable_param_group_ref      });
impl_param_collector!(Software {
    cv: cv_param,
    user: user_params,
    sw: software_param
});
impl_param_collector!(ProcessingMethod { cv: cv_param,  user: user_param,   ref: referenceable_param_group_ref      });
impl_param_collector!(ScanSettings     { cv: cv_params, user: user_params,  ref: referenceable_param_group_refs     });
impl_param_collector!(Target           { cv: cv_params, user: user_params,  ref: referenceable_param_group_refs     });
impl_param_collector!(Run              { cv: cv_params, user: user_params,  ref: referenceable_param_group_refs     });
impl_param_collector!(SpectrumDescription { cv: cv_params, user: user_params, ref: referenceable_param_group_refs   });
impl_param_collector!(Scan             { cv: cv_params, user: user_params,  ref: referenceable_param_group_refs     });
impl_param_collector!(ScanWindow {
    cv: cv_params,
    user: user_params
});
impl_param_collector!(IsolationWindow  { cv: cv_params, user: user_params,  ref: referenceable_param_group_refs     });
impl_param_collector!(SelectedIon      { cv: cv_params, user: user_params,  ref: referenceable_param_group_refs     });
impl_param_collector!(Activation       { cv: cv_params, user: user_params,  ref: referenceable_param_group_refs     });
impl_param_collector!(BinaryDataArray  { cv: cv_params, user: user_params,  ref: referenceable_param_group_refs     });
impl_param_collector!(Chromatogram     { cv: cv_params, user: user_params,  ref: referenceable_param_group_refs     });

impl ParamCollector for Spectrum {
    fn receive_cv(&mut self, p: CvParam) {
        if self.ms_level.is_none() && p.accession.as_deref() == Some("MS:1000511") {
            self.ms_level = p.value.as_deref().and_then(|v| v.parse().ok());
        }
        self.cv_params.push(p);
    }
    fn receive_user(&mut self, p: UserParam) {
        self.user_params.push(p);
    }
    fn receive_ref_group(&mut self, p: ReferenceableParamGroupRef) {
        self.referenceable_param_group_refs.push(p);
    }
}

impl ParamCollector for ScanList {
    fn receive_cv(&mut self, p: CvParam) {
        self.cv_params.push(p);
    }
    fn receive_user(&mut self, p: UserParam) {
        self.user_params.push(p);
    }
}
