const express = require('express');
const router = express.Router();
const mongoose = require('mongoose');
const QTD_Boarded_91M = mongoose.model('QTD_Boarded_91M',mongoose.Schema({}),collection='qtd_boarded_91m');

router.get('/latest', async (req,res) => {
    try {
        const qtd = await QTD_Boarded_91M.find().limit(1).sort({'Time':-1});
        res.json(qtd);
    } catch (err) {
        res.json({message:err});
    }
});

router.get('/last_hour', async (req,res) => {
    try {
        const qtd = await QTD_Boarded_91M.find().limit(60).sort({'Time':-1});
        res.json(qtd);
    } catch (err) {
        res.json({message:err});
    }
});

module.exports = router;