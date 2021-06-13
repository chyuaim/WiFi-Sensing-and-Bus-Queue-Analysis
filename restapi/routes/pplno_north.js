const express = require('express');
const router = express.Router();
const mongoose = require('mongoose');
const Pplno_North = mongoose.model('Pplno_North',mongoose.Schema({}),collection='pplno_north');

router.get('/latest', async (req,res) => {
    try {
        const pplno = await Pplno_North.find().limit(1).sort({'Time':-1});
        res.json(pplno);
    } catch (err) {
        res.json({message:err});
    }
});

router.get('/last_hour', async (req,res) => {
    try {
        const pplno = await Pplno_North.find().limit(720).sort({'Time':-1});
        res.json(pplno);
    } catch (err) {
        res.json({message:err});
    }
});
 
module.exports = router;