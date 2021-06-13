const express = require('express');
const router = express.Router();
const mongoose = require('mongoose');
const QTD_Current_91P = mongoose.model('QTD_Current_91P',mongoose.Schema({}),collection='qtd_current_91p');

router.get('/latest', async (req,res) => {
    try {
        const qtd = await QTD_Current_91P.find().limit(1).sort({'Time':-1});
        res.json(qtd);
    } catch (err) {
        res.json({message:err});
    }
});

router.get('/last_hour', async (req,res) => {
    try {
        const qtd = await QTD_Current_91P.find().limit(60).sort({'Time':-1});
        res.json(qtd);
    } catch (err) {
        res.json({message:err});
    }
});
 
module.exports = router;