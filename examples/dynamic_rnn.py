from __future__ import absolute_import, division, print_function

# Taken from https://github.com/aymericdamien/TensorFlow-Examples/

# Import TensorFlow v2.
import tensorflow as tf
from tensorflow.keras import Model, layers
import numpy as np
import random

from simvue import Simvue

# Dataset parameters.
num_classes = 2 # linear sequence or not.
seq_max_len = 20 # Maximum sequence length.
seq_min_len = 5 # Minimum sequence length (before padding).
masking_val = -1 # -1 will represents the mask and be used to pad sequences to a common max length.
max_value = 10000 # Maximum int value.


# Training Parameters
learning_rate = 0.001
training_steps = 2000
batch_size = 64

# Network Parameters
num_units = 32 # number of neurons for the LSTM layer.

run = Simvue()
run.init(metadata={'dataset.num_classes': num_classes,
                   'dataset.seq_max_len': seq_max_len,
                   'dataset.seq_min_len': seq_min_len,
                   'dataset.masking_val': masking_val,
                   'training.learning_rate': learning_rate,
                   'training.training_steps': training_steps,
                   'training.batch_size': batch_size,
                   'network.num_units': num_units},
         description="TensorFlow 2.0 implementation of a Recurrent Neural Network (LSTM) that performs dynamic "
                     "computation over sequences with variable length. This example is using a toy dataset to "
                     "classify linear sequences. The generated sequences have variable length.")

# ====================
#  TOY DATA GENERATOR
# ====================

def toy_sequence_data():
    """ Generate sequence of data with dynamic length.
    This function generates toy samples for training:
    - Class 0: linear sequences (i.e. [1, 2, 3, 4, ...])
    - Class 1: random sequences (i.e. [9, 3, 10, 7,...])

    NOTICE:
    We have to pad each sequence to reach 'seq_max_len' for TensorFlow
    consistency (we cannot feed a numpy array with inconsistent
    dimensions). The dynamic calculation will then be perform and ignore
    the masked value (here -1).
    """
    while True:
        # Set variable sequence length.
        seq_len = random.randint(seq_min_len, seq_max_len)
        rand_start = random.randint(0, max_value - seq_len)
        # Add a random or linear int sequence (50% prob).
        if random.random() < .5:
            # Generate a linear sequence.
            seq = np.arange(start=rand_start, stop=rand_start+seq_len)
            # Rescale values to [0., 1.].
            seq = seq / max_value
            # Pad sequence until the maximum length for dimension consistency.
            # Masking value: -1.
            seq = np.pad(seq, mode='constant', pad_width=(0, seq_max_len-seq_len), constant_values=masking_val)
            label = 0
        else:
            # Generate a random sequence.
            seq = np.random.randint(max_value, size=seq_len)
            # Rescale values to [0., 1.].
            seq = seq / max_value
            # Pad sequence until the maximum length for dimension consistency.
            # Masking value: -1.
            seq = np.pad(seq, mode='constant', pad_width=(0, seq_max_len-seq_len), constant_values=masking_val)
            label = 1
        yield np.array(seq, dtype=np.float32), np.array(label, dtype=np.float32)

# Use tf.data API to shuffle and batch data.
train_data = tf.data.Dataset.from_generator(toy_sequence_data, output_types=(tf.float32, tf.float32))
train_data = train_data.repeat().shuffle(5000).batch(batch_size).prefetch(1)

# Create LSTM Model.
class LSTM(Model):
    # Set layers.
    def __init__(self):
        super(LSTM, self).__init__()
        # Define a Masking Layer with -1 as mask.
        self.masking = layers.Masking(mask_value=masking_val)
        # Define a LSTM layer to be applied over the Masking layer.
        # Dynamic computation will automatically be performed to ignore -1 values.
        self.lstm = layers.LSTM(units=num_units)
        # Output fully connected layer (2 classes: linear or random seq).
        self.out = layers.Dense(num_classes)

    # Set forward pass.
    def call(self, x, is_training=False):
        # A RNN Layer expects a 3-dim input (batch_size, seq_len, num_features).
        x = tf.reshape(x, shape=[-1, seq_max_len, 1])
        # Apply Masking layer.
        x = self.masking(x)
        # Apply LSTM layer.
        x = self.lstm(x)
        # Apply output layer.
        x = self.out(x)
        if not is_training:
            # tf cross entropy expect logits without softmax, so only
            # apply softmax when not training.
            x = tf.nn.softmax(x)
        return x

# Build LSTM model.
lstm_net = LSTM()

# Cross-Entropy Loss.
# Note that this will apply 'softmax' to the logits.
def cross_entropy_loss(x, y):
    # Convert labels to int 64 for tf cross-entropy function.
    y = tf.cast(y, tf.int64)
    # Apply softmax to logits and compute cross-entropy.
    loss = tf.nn.sparse_softmax_cross_entropy_with_logits(labels=y, logits=x)
    # Average loss across the batch.
    return tf.reduce_mean(loss)

# Accuracy metric.
def accuracy(y_pred, y_true):
    # Predicted class is the index of highest score in prediction vector (i.e. argmax).
    correct_prediction = tf.equal(tf.argmax(y_pred, 1), tf.cast(y_true, tf.int64))
    return tf.reduce_mean(tf.cast(correct_prediction, tf.float32), axis=-1)

# Adam optimizer.
optimizer = tf.optimizers.Adam(learning_rate)

# Optimization process.
def run_optimization(x, y):
    # Wrap computation inside a GradientTape for automatic differentiation.
    with tf.GradientTape() as g:
        # Forward pass.
        pred = lstm_net(x, is_training=True)
        # Compute loss.
        loss = cross_entropy_loss(pred, y)

    # Variables to update, i.e. trainable variables.
    trainable_variables = lstm_net.trainable_variables

    # Compute gradients.
    gradients = g.gradient(loss, trainable_variables)

    # Update weights following gradients.
    optimizer.apply_gradients(zip(gradients, trainable_variables))

# Run training for the given number of steps.
for step, (batch_x, batch_y) in enumerate(train_data.take(training_steps), 1):
    # Run the optimization to update W and b values.
    run_optimization(batch_x, batch_y)

    pred = lstm_net(batch_x, is_training=True)
    loss = cross_entropy_loss(pred, batch_y)
    acc = accuracy(pred, batch_y)
    run.log_metrics({'loss': float(loss), 'accuracy': float(acc)})

run.close()
