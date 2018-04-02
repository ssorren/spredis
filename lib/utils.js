const textUtil = require('./text');

function invertBinaryString(x){
	//can't use bitwise operators since javasctipt operators convert everthing to 32 bit integers
	let v = '';
    for(let i = 0, l = x.length; i < l; i++) {
    	v += (x.charAt(i) === '0' ? '1' : '0');
    }
    return v;
}

var INVERSE_CHAR = {};
for (var i = 0; i < 128; i++) {
	INVERSE_CHAR[String.fromCharCode(i)] = String.fromCharCode(127 - i);
}
function alphaString(x) {
	return Buffer.from(x, 'utf8').toString('ascii');
}
function invertAlphaString(x){
	let v = '';
    for(let i = 0, l = x.length; i < l; i++) {
    	v += INVERSE_CHAR[x.charAt(i)] ;
    }
    return v;
}

var SPACE_INVERSE = invertAlphaString(String.fromCharCode(0));

function checkStringLen(str, maxLen) {
	return str.length > maxLen ? str.substr(0, maxLen) : str;
}
// created a signed integer in binary string form. this will be sort proof.
// if the number is negative, flip the bits
function binaryString(number, padTo) {
	let neg = (number < 0);
	let pad = padTo - 1; //have to account for sign
	number = Math.abs(number).toString(2);
	if (neg) {
		return checkStringLen( '0' +  invertBinaryString(number).padStart(pad, '1'), padTo );
	}
	return checkStringLen( '1' + number.padStart(pad, '0'), padTo );
}

/*
function binAscAlphaSort(integer, padTo) {
	return binaryString(integer, padTo);
}

function binDescAlphaSort(integer, padTo) {
	return binaryString(integer * -1, padTo);
}

function strAscAlphaSort(str, padTo) {
	str = (str || '').toLowerCase();
	return checkStringLen(alphaString(textUtil.internationalize(str)), padTo).padEnd(padTo, ' ');;
}

function strDescAlphaSort(str, padTo) {
	str = (str || '').toLowerCase();
	return invertAlphaString(checkStringLen( alphaString(textUtil.internationalize(str)), padTo)).padEnd(padTo, SPACE_INVERSE);
}
*/
function longToID(/*long*/long) {
 	return  long.toString(16);
}

function waitAwhile(duration) {
	return new Promise( (resolve, reject) => {
		setTimeout( ()=> {
			resolve();
		}, duration || 1000);
	});
}

module.exports = {

	// integerToAscAlphaSort: binAscAlphaSort,
	// integerToDescAlphaSort: binDescAlphaSort,
	// strAscAlphaSort: strAscAlphaSort,
	// strDescAlphaSort: strDescAlphaSort,
	longToID: longToID,
	waitAwhile: waitAwhile,
	pause: waitAwhile
}