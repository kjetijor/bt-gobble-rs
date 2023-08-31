#![allow(dead_code)]
// bitvec seems to do exactly what I want!

#[derive(Debug,PartialEq)]
enum Endianness {
    Big,
    Little,
}

#[derive(Debug,PartialEq)]
enum Output {
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
}

#[derive(Debug,PartialEq)]
struct Field {
    name: String,
    bit_position: usize,
    signed: bool,
    endianness: Endianness,
    width: usize,
//    output_type: OutputType,
}

#[derive(Debug,PartialEq)]
struct FieldValue {
    name: String,
//    value: Value,
}

#[cfg(test)]
mod tests {
    use bitvec::prelude::*;
    #[test]
    fn test_bs() {
        let input: Vec<u8> = vec![ 0b1010_0110 ];

        let bs = input.view_bits::<Msb0>();
        let i1 = bs[0..3].load_be::<u8>();
        assert_eq!(i1, 0b101u8);
        assert_eq!(bs[4..8].load_be::<u8>(), 0b110u8);
        let i2: Vec<u8> = vec![ 0b0000_0011, 0b1111_1111, 0b1111_1100];
        let bs2 = i2.view_bits::<Msb0>();
        assert_eq!(bs2[6..20].load_be::<i16>(), -1i16);
        assert_eq!(bs2[6..13].load_be::<i16>(), -1i16); // this is odd...
//        assert_eq!(bs2[6..12].load_be::<i16>(), 1i16);
//        assert_eq!(bs2[6..8].load_be::<i16>(), 1);
    }
}