const express = require('express');
const router = express.Router();
const mongoose = require('mongoose');
const Pplno_South = mongoose.model('Pplno_South',mongoose.Schema({}),collection='pplno_south');

router.get('/latest', async (req,res) => {
    try {
        const posts = await Pplno_South.find().limit(1).sort({'Time':-1});
        res.json(posts);
    } catch (err) {
        res.json({message:err});
    }
});

router.get('/last_hour', async (req,res) => {
    try {
        const posts = await Pplno_South.find().limit(720).sort({'Time':-1});
        res.json(posts);
    } catch (err) {
        res.json({message:err});
    }
});

module.exports = router;