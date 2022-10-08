// This file is generated by rust-protobuf 3.1.0. Do not edit
// .proto file is parsed by protoc --rust-out=...
// @generated

// https://github.com/rust-lang/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy::all)]

#![allow(unused_attributes)]
#![cfg_attr(rustfmt, rustfmt::skip)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unused_results)]
#![allow(unused_mut)]

//! Generated file from `feast/core/DynamoDBTable.proto`

/// Generated files are compatible only with the same version
/// of protobuf runtime.
const _PROTOBUF_VERSION_CHECK: () = ::protobuf::VERSION_3_1_0;

///  Represents a DynamoDB table
#[derive(PartialEq,Clone,Default,Debug)]
// @@protoc_insertion_point(message:feast.core.DynamoDBTable)
pub struct DynamoDBTable {
    // message fields
    ///  Name of the table
    // @@protoc_insertion_point(field:feast.core.DynamoDBTable.name)
    pub name: ::std::string::String,
    ///  Region of the table
    // @@protoc_insertion_point(field:feast.core.DynamoDBTable.region)
    pub region: ::std::string::String,
    // special fields
    // @@protoc_insertion_point(special_field:feast.core.DynamoDBTable.special_fields)
    pub special_fields: ::protobuf::SpecialFields,
}

impl<'a> ::std::default::Default for &'a DynamoDBTable {
    fn default() -> &'a DynamoDBTable {
        <DynamoDBTable as ::protobuf::Message>::default_instance()
    }
}

impl DynamoDBTable {
    pub fn new() -> DynamoDBTable {
        ::std::default::Default::default()
    }

    fn generated_message_descriptor_data() -> ::protobuf::reflect::GeneratedMessageDescriptorData {
        let mut fields = ::std::vec::Vec::with_capacity(2);
        let mut oneofs = ::std::vec::Vec::with_capacity(0);
        fields.push(::protobuf::reflect::rt::v2::make_simpler_field_accessor::<_, _>(
            "name",
            |m: &DynamoDBTable| { &m.name },
            |m: &mut DynamoDBTable| { &mut m.name },
        ));
        fields.push(::protobuf::reflect::rt::v2::make_simpler_field_accessor::<_, _>(
            "region",
            |m: &DynamoDBTable| { &m.region },
            |m: &mut DynamoDBTable| { &mut m.region },
        ));
        ::protobuf::reflect::GeneratedMessageDescriptorData::new_2::<DynamoDBTable>(
            "DynamoDBTable",
            fields,
            oneofs,
        )
    }
}

impl ::protobuf::Message for DynamoDBTable {
    const NAME: &'static str = "DynamoDBTable";

    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream<'_>) -> ::protobuf::Result<()> {
        while let Some(tag) = is.read_raw_tag_or_eof()? {
            match tag {
                10 => {
                    self.name = is.read_string()?;
                },
                18 => {
                    self.region = is.read_string()?;
                },
                tag => {
                    ::protobuf::rt::read_unknown_or_skip_group(tag, is, self.special_fields.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u64 {
        let mut my_size = 0;
        if !self.name.is_empty() {
            my_size += ::protobuf::rt::string_size(1, &self.name);
        }
        if !self.region.is_empty() {
            my_size += ::protobuf::rt::string_size(2, &self.region);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.special_fields.unknown_fields());
        self.special_fields.cached_size().set(my_size as u32);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream<'_>) -> ::protobuf::Result<()> {
        if !self.name.is_empty() {
            os.write_string(1, &self.name)?;
        }
        if !self.region.is_empty() {
            os.write_string(2, &self.region)?;
        }
        os.write_unknown_fields(self.special_fields.unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn special_fields(&self) -> &::protobuf::SpecialFields {
        &self.special_fields
    }

    fn mut_special_fields(&mut self) -> &mut ::protobuf::SpecialFields {
        &mut self.special_fields
    }

    fn new() -> DynamoDBTable {
        DynamoDBTable::new()
    }

    fn clear(&mut self) {
        self.name.clear();
        self.region.clear();
        self.special_fields.clear();
    }

    fn default_instance() -> &'static DynamoDBTable {
        static instance: DynamoDBTable = DynamoDBTable {
            name: ::std::string::String::new(),
            region: ::std::string::String::new(),
            special_fields: ::protobuf::SpecialFields::new(),
        };
        &instance
    }
}

impl ::protobuf::MessageFull for DynamoDBTable {
    fn descriptor() -> ::protobuf::reflect::MessageDescriptor {
        static descriptor: ::protobuf::rt::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::rt::Lazy::new();
        descriptor.get(|| file_descriptor().message_by_package_relative_name("DynamoDBTable").unwrap()).clone()
    }
}

impl ::std::fmt::Display for DynamoDBTable {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for DynamoDBTable {
    type RuntimeType = ::protobuf::reflect::rt::RuntimeTypeMessage<Self>;
}

static file_descriptor_proto_data: &'static [u8] = b"\
    \n\x1efeast/core/DynamoDBTable.proto\x12\nfeast.core\";\n\rDynamoDBTable\
    \x12\x12\n\x04name\x18\x01\x20\x01(\tR\x04name\x12\x16\n\x06region\x18\
    \x02\x20\x01(\tR\x06regionBW\n\x10feast.proto.coreB\x12DynamoDBTableProt\
    oZ/github.com/feast-dev/feast/go/protos/feast/coreJ\x85\x07\n\x06\x12\
    \x04\x10\0\x1e\x01\n\xe0\x04\n\x01\x0c\x12\x03\x10\0\x122\xd5\x04\n\x20*\
    \x20Copyright\x202021\x20The\x20Feast\x20Authors\n\x20*\n\x20*\x20Licens\
    ed\x20under\x20the\x20Apache\x20License,\x20Version\x202.0\x20(the\x20\"\
    License\");\n\x20*\x20you\x20may\x20not\x20use\x20this\x20file\x20except\
    \x20in\x20compliance\x20with\x20the\x20License.\n\x20*\x20You\x20may\x20\
    obtain\x20a\x20copy\x20of\x20the\x20License\x20at\n\x20*\n\x20*\x20\x20\
    \x20\x20\x20https://www.apache.org/licenses/LICENSE-2.0\n\x20*\n\x20*\
    \x20Unless\x20required\x20by\x20applicable\x20law\x20or\x20agreed\x20to\
    \x20in\x20writing,\x20software\n\x20*\x20distributed\x20under\x20the\x20\
    License\x20is\x20distributed\x20on\x20an\x20\"AS\x20IS\"\x20BASIS,\n\x20\
    *\x20WITHOUT\x20WARRANTIES\x20OR\x20CONDITIONS\x20OF\x20ANY\x20KIND,\x20\
    either\x20express\x20or\x20implied.\n\x20*\x20See\x20the\x20License\x20f\
    or\x20the\x20specific\x20language\x20governing\x20permissions\x20and\n\
    \x20*\x20limitations\x20under\x20the\x20License.\n\n\n\x08\n\x01\x02\x12\
    \x03\x12\0\x13\n\x08\n\x01\x08\x12\x03\x13\0)\n\t\n\x02\x08\x01\x12\x03\
    \x13\0)\n\x08\n\x01\x08\x12\x03\x14\03\n\t\n\x02\x08\x08\x12\x03\x14\03\
    \n\x08\n\x01\x08\x12\x03\x15\0F\n\t\n\x02\x08\x0b\x12\x03\x15\0F\n)\n\
    \x02\x04\0\x12\x04\x18\0\x1e\x01\x1a\x1d\x20Represents\x20a\x20DynamoDB\
    \x20table\n\n\n\n\x03\x04\0\x01\x12\x03\x18\x08\x15\n\x20\n\x04\x04\0\
    \x02\0\x12\x03\x1a\x04\x14\x1a\x13\x20Name\x20of\x20the\x20table\n\n\x0c\
    \n\x05\x04\0\x02\0\x05\x12\x03\x1a\x04\n\n\x0c\n\x05\x04\0\x02\0\x01\x12\
    \x03\x1a\x0b\x0f\n\x0c\n\x05\x04\0\x02\0\x03\x12\x03\x1a\x12\x13\n\"\n\
    \x04\x04\0\x02\x01\x12\x03\x1d\x04\x16\x1a\x15\x20Region\x20of\x20the\
    \x20table\n\n\x0c\n\x05\x04\0\x02\x01\x05\x12\x03\x1d\x04\n\n\x0c\n\x05\
    \x04\0\x02\x01\x01\x12\x03\x1d\x0b\x11\n\x0c\n\x05\x04\0\x02\x01\x03\x12\
    \x03\x1d\x14\x15b\x06proto3\
";

/// `FileDescriptorProto` object which was a source for this generated file
fn file_descriptor_proto() -> &'static ::protobuf::descriptor::FileDescriptorProto {
    static file_descriptor_proto_lazy: ::protobuf::rt::Lazy<::protobuf::descriptor::FileDescriptorProto> = ::protobuf::rt::Lazy::new();
    file_descriptor_proto_lazy.get(|| {
        ::protobuf::Message::parse_from_bytes(file_descriptor_proto_data).unwrap()
    })
}

/// `FileDescriptor` object which allows dynamic access to files
pub fn file_descriptor() -> &'static ::protobuf::reflect::FileDescriptor {
    static generated_file_descriptor_lazy: ::protobuf::rt::Lazy<::protobuf::reflect::GeneratedFileDescriptor> = ::protobuf::rt::Lazy::new();
    static file_descriptor: ::protobuf::rt::Lazy<::protobuf::reflect::FileDescriptor> = ::protobuf::rt::Lazy::new();
    file_descriptor.get(|| {
        let generated_file_descriptor = generated_file_descriptor_lazy.get(|| {
            let mut deps = ::std::vec::Vec::with_capacity(0);
            let mut messages = ::std::vec::Vec::with_capacity(1);
            messages.push(DynamoDBTable::generated_message_descriptor_data());
            let mut enums = ::std::vec::Vec::with_capacity(0);
            ::protobuf::reflect::GeneratedFileDescriptor::new_generated(
                file_descriptor_proto(),
                deps,
                messages,
                enums,
            )
        });
        ::protobuf::reflect::FileDescriptor::new_generated_2(generated_file_descriptor)
    })
}