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

//! Generated file from `feast/types/EntityKey.proto`

/// Generated files are compatible only with the same version
/// of protobuf runtime.
const _PROTOBUF_VERSION_CHECK: () = ::protobuf::VERSION_3_1_0;

#[derive(PartialEq,Clone,Default,Debug)]
// @@protoc_insertion_point(message:feast.types.EntityKey)
pub struct EntityKey {
    // message fields
    // @@protoc_insertion_point(field:feast.types.EntityKey.join_keys)
    pub join_keys: ::std::vec::Vec<::std::string::String>,
    // @@protoc_insertion_point(field:feast.types.EntityKey.entity_values)
    pub entity_values: ::std::vec::Vec<super::Value::Value>,
    // special fields
    // @@protoc_insertion_point(special_field:feast.types.EntityKey.special_fields)
    pub special_fields: ::protobuf::SpecialFields,
}

impl<'a> ::std::default::Default for &'a EntityKey {
    fn default() -> &'a EntityKey {
        <EntityKey as ::protobuf::Message>::default_instance()
    }
}

impl EntityKey {
    pub fn new() -> EntityKey {
        ::std::default::Default::default()
    }

    fn generated_message_descriptor_data() -> ::protobuf::reflect::GeneratedMessageDescriptorData {
        let mut fields = ::std::vec::Vec::with_capacity(2);
        let mut oneofs = ::std::vec::Vec::with_capacity(0);
        fields.push(::protobuf::reflect::rt::v2::make_vec_simpler_accessor::<_, _>(
            "join_keys",
            |m: &EntityKey| { &m.join_keys },
            |m: &mut EntityKey| { &mut m.join_keys },
        ));
        fields.push(::protobuf::reflect::rt::v2::make_vec_simpler_accessor::<_, _>(
            "entity_values",
            |m: &EntityKey| { &m.entity_values },
            |m: &mut EntityKey| { &mut m.entity_values },
        ));
        ::protobuf::reflect::GeneratedMessageDescriptorData::new_2::<EntityKey>(
            "EntityKey",
            fields,
            oneofs,
        )
    }
}

impl ::protobuf::Message for EntityKey {
    const NAME: &'static str = "EntityKey";

    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream<'_>) -> ::protobuf::Result<()> {
        while let Some(tag) = is.read_raw_tag_or_eof()? {
            match tag {
                10 => {
                    self.join_keys.push(is.read_string()?);
                },
                18 => {
                    self.entity_values.push(is.read_message()?);
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
        for value in &self.join_keys {
            my_size += ::protobuf::rt::string_size(1, &value);
        };
        for value in &self.entity_values {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint64_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.special_fields.unknown_fields());
        self.special_fields.cached_size().set(my_size as u32);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream<'_>) -> ::protobuf::Result<()> {
        for v in &self.join_keys {
            os.write_string(1, &v)?;
        };
        for v in &self.entity_values {
            ::protobuf::rt::write_message_field_with_cached_size(2, v, os)?;
        };
        os.write_unknown_fields(self.special_fields.unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn special_fields(&self) -> &::protobuf::SpecialFields {
        &self.special_fields
    }

    fn mut_special_fields(&mut self) -> &mut ::protobuf::SpecialFields {
        &mut self.special_fields
    }

    fn new() -> EntityKey {
        EntityKey::new()
    }

    fn clear(&mut self) {
        self.join_keys.clear();
        self.entity_values.clear();
        self.special_fields.clear();
    }

    fn default_instance() -> &'static EntityKey {
        static instance: EntityKey = EntityKey {
            join_keys: ::std::vec::Vec::new(),
            entity_values: ::std::vec::Vec::new(),
            special_fields: ::protobuf::SpecialFields::new(),
        };
        &instance
    }
}

impl ::protobuf::MessageFull for EntityKey {
    fn descriptor() -> ::protobuf::reflect::MessageDescriptor {
        static descriptor: ::protobuf::rt::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::rt::Lazy::new();
        descriptor.get(|| file_descriptor().message_by_package_relative_name("EntityKey").unwrap()).clone()
    }
}

impl ::std::fmt::Display for EntityKey {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for EntityKey {
    type RuntimeType = ::protobuf::reflect::rt::RuntimeTypeMessage<Self>;
}

static file_descriptor_proto_data: &'static [u8] = b"\
    \n\x1bfeast/types/EntityKey.proto\x12\x0bfeast.types\x1a\x17feast/types/\
    Value.proto\"a\n\tEntityKey\x12\x1b\n\tjoin_keys\x18\x01\x20\x03(\tR\x08\
    joinKeys\x127\n\rentity_values\x18\x02\x20\x03(\x0b2\x12.feast.types.Val\
    ueR\x0centityValuesBU\n\x11feast.proto.typesB\x0eEntityKeyProtoZ0github.\
    com/feast-dev/feast/go/protos/feast/typesJ\xc6\x06\n\x06\x12\x04\x10\0\
    \x1d\x01\n\xc5\x04\n\x01\x0c\x12\x03\x10\0\x122\xba\x04\n\x20Copyright\
    \x202018\x20The\x20Feast\x20Authors\n\n\x20Licensed\x20under\x20the\x20A\
    pache\x20License,\x20Version\x202.0\x20(the\x20\"License\");\n\x20you\
    \x20may\x20not\x20use\x20this\x20file\x20except\x20in\x20compliance\x20w\
    ith\x20the\x20License.\n\x20You\x20may\x20obtain\x20a\x20copy\x20of\x20t\
    he\x20License\x20at\n\n\x20\x20\x20\x20\x20https://www.apache.org/licens\
    es/LICENSE-2.0\n\n\x20Unless\x20required\x20by\x20applicable\x20law\x20o\
    r\x20agreed\x20to\x20in\x20writing,\x20software\n\x20distributed\x20unde\
    r\x20the\x20License\x20is\x20distributed\x20on\x20an\x20\"AS\x20IS\"\x20\
    BASIS,\n\x20WITHOUT\x20WARRANTIES\x20OR\x20CONDITIONS\x20OF\x20ANY\x20KI\
    ND,\x20either\x20express\x20or\x20implied.\n\x20See\x20the\x20License\
    \x20for\x20the\x20specific\x20language\x20governing\x20permissions\x20an\
    d\n\x20limitations\x20under\x20the\x20License.\n\n\t\n\x02\x03\0\x12\x03\
    \x12\0!\n\x08\n\x01\x02\x12\x03\x14\0\x14\n\x08\n\x01\x08\x12\x03\x16\0*\
    \n\t\n\x02\x08\x01\x12\x03\x16\0*\n\x08\n\x01\x08\x12\x03\x17\0/\n\t\n\
    \x02\x08\x08\x12\x03\x17\0/\n\x08\n\x01\x08\x12\x03\x18\0G\n\t\n\x02\x08\
    \x0b\x12\x03\x18\0G\n\n\n\x02\x04\0\x12\x04\x1a\0\x1d\x01\n\n\n\x03\x04\
    \0\x01\x12\x03\x1a\x08\x11\n\x0b\n\x04\x04\0\x02\0\x12\x03\x1b\x04\"\n\
    \x0c\n\x05\x04\0\x02\0\x04\x12\x03\x1b\x04\x0c\n\x0c\n\x05\x04\0\x02\0\
    \x05\x12\x03\x1b\r\x13\n\x0c\n\x05\x04\0\x02\0\x01\x12\x03\x1b\x14\x1d\n\
    \x0c\n\x05\x04\0\x02\0\x03\x12\x03\x1b\x20!\n\x0b\n\x04\x04\0\x02\x01\
    \x12\x03\x1c\x041\n\x0c\n\x05\x04\0\x02\x01\x04\x12\x03\x1c\x04\x0c\n\
    \x0c\n\x05\x04\0\x02\x01\x06\x12\x03\x1c\r\x1e\n\x0c\n\x05\x04\0\x02\x01\
    \x01\x12\x03\x1c\x1f,\n\x0c\n\x05\x04\0\x02\x01\x03\x12\x03\x1c/0b\x06pr\
    oto3\
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
            let mut deps = ::std::vec::Vec::with_capacity(1);
            deps.push(super::Value::file_descriptor().clone());
            let mut messages = ::std::vec::Vec::with_capacity(1);
            messages.push(EntityKey::generated_message_descriptor_data());
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